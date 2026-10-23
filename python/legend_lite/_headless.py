"""DataCube's own code, headless: its non-screen bundle run in V8 (mini-racer), planning through legend-lite's native
library and querying Python's DuckDB, with no WebAssembly anywhere in the process
(docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3, §5).

DataCube's planner talks to a worker (datacube/src/planner-protocol.ts); here the host is that worker, and it answers
each request kind with the native library's function that delegates to the same ``Boundary`` function the tab's
WebAssembly export does. ``KINDS`` (legend_lite._planner) is that table, the one place it is written: the engine
answers by it, and checks by it, before anything runs, that the bundle sends no kind it lacks and that the library
exports every function it names.

The engine runs on an event loop of its own, on its own thread: a caller's thread (a notebook's included, whose own
loop is running) posts each call to it and waits, so the API is synchronous and never touches the caller's loop.

WHAT A CALL ASKS OF PYTHON IS ANSWERED ON THE ENGINE'S THREAD, WAITING ON NO OTHER (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
D15): a call made inside a read of the frames (``Frames.reading``) queries through that read's own cursor, and what it asks
of Python's engine (``engine``: a grid's rows planned and run where they are) is answered over that read too; the engine's
thread is one of the compiler's (``_pool.library_thread``), so a library call it makes runs there. A call is made in its
turn (``turn``), which a caller that writes the frames' database through DataCube's code takes before it holds the frames
(the order every lock is taken in: the turn, then the frames', then the warehouse's): no one holds a lock a call's answer
takes while waiting for the call's turn.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import io
import json
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Coroutine, Iterator
from pathlib import Path
from typing import Any

from . import _host
from ._library import Library, library
from ._pool import library_thread

from ._planner import KINDS, answer  # noqa: F401 -- the planner protocol's table, answered here and by the engine

# WebCrypto's digests, by the names crypto.subtle.digest takes
_DIGESTS = {'SHA-1': 'sha1', 'SHA-256': 'sha256', 'SHA-384': 'sha384', 'SHA-512': 'sha512'}

# how much of V8's heap the engine may use: past it, the call that needs more fails, not the process
DEFAULT_MEMORY_LIMIT = 2 * 1024 ** 3

# a server is given this long to answer one fetch (seconds)
_FETCH_TIMEOUT = 60

# the paths this host answers a fetch at itself: its model home's SDLC and Depot, and legend-lite's API
_HOME_APIS = (('/sdlc/api/', 'sdlc'), ('/depot/api/', 'depot'))
_LITE_API = '/api/lite/v1/'


def answer_fetch(method: str, url: str, body: str | None) -> tuple[int, str]:
    """A fetch DataCube's code makes headless (its model actions' SDLC and model writer: calls.ts ``fetch``), answered
    as this process answers it: a path of its own -- this user's model home (``/sdlc/api/...``, ``/depot/api/...``:
    the native library's SDLC and Depot, as the engine's routes answer them) or legend-lite's API (``/api/lite/v1/...``:
    the model writer, the native library's) -- or a server's address, over HTTP. Its status and its body; a failure of
    the library's said in its words (500), never raised into DataCube's code."""
    parts = urllib.parse.urlsplit(url)
    if parts.scheme in ('http', 'https'):
        request = urllib.request.Request(url, data=None if body is None else body.encode('utf-8'), method=method,
                                         headers={} if body is None else {'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(request, timeout=_FETCH_TIMEOUT) as answered:
                return answered.status, answered.read().decode('utf-8')
        except urllib.error.HTTPError as refused:
            return refused.code, refused.read().decode('utf-8', errors='replace')
    if parts.scheme or parts.netloc:
        return 400, json.dumps({'message': f'this host fetches over HTTP, or answers a path of its own: not {url}'})
    for root, kind in _HOME_APIS:
        if parts.path.startswith(root):
            from ._home import home_call, model_home_folder
            target = '/' + parts.path[len(root):] + (f'?{parts.query}' if parts.query else '')
            try:
                return home_call(model_home_folder(), kind, method, target, body)
            except OSError as refused:
                return 500, json.dumps({'message': str(refused)})
    if parts.path.startswith(_LITE_API):
        from . import compiler
        answered = compiler.pure_v1(parts.path, parts.query, body or '')
        return answered.status, answered.body
    return 404, json.dumps({'message': f"this host answers its model home ({', '.join(r for r, _ in _HOME_APIS)}) and "
                                       f'legend-lite\'s API ({_LITE_API}) at a path: not {url}'})


class InterfaceMismatch(RuntimeError):
    """The bundle and the native library do not agree on the planner's requests: both sides named."""


def check_interface(bundle_kinds: list[str], lib: Library) -> None:
    """The bundle's request kinds against ``KINDS``, and each function ``KINDS`` names against the library's exports."""
    sent, answered = set(bundle_kinds), set(KINDS)
    missing = sorted(k for k in KINDS if not lib.exports(KINDS[k][0]))
    if sent != answered or missing:
        raise InterfaceMismatch(
            "DataCube's bundle and legend-lite's native library do not agree on the planner's requests: "
            f'the bundle sends {sorted(sent)}; this host answers {sorted(answered)}'
            + (f'; the library exports none of {[KINDS[k][0] for k in missing]}' if missing else ''))


class Headless:
    """DataCube's non-screen code in one V8 context, with its planner and its database. One per process is the intent
    (one state owner, as on screen); its calls run one at a time, in the order they come. A call that runs V8 past its
    memory cap fails, and the engine is then ``broken``: its owner makes another. ``close()`` (or a ``with`` block)
    ends it."""

    def __init__(self, bundle: str, *, connection: Any = None, folder: str | Path | None = None,
                 memory_limit: int = DEFAULT_MEMORY_LIMIT, lib: Library | None = None,
                 reading: Callable[[], Any] | None = None) -> None:
        """``bundle`` is a headless bundle's script. ``connection`` the duckdb connection its queries run on (a Frames'
        one, whose tables a page's grids read; a new database of the engine's own when not given, closed with it).
        ``folder`` where the files DataCube registers are written (a temporary folder of the engine's own when not
        given); a database of the engine's own reads relative names from it. A connection given is left as it is: a
        file registered meanwhile is in ``folder``, which that database does not search (no grid registers one).
        ``reading``: the read of the frames the calling thread is in, if any (``Frames.reading_now``): a call made in one
        queries through it."""
        import duckdb  # the database: the datacube extra's, never the compiler's

        self._lib = lib or library()
        self._own_folder = tempfile.TemporaryDirectory(prefix='legend-lite-datacube-') if folder is None else None
        self._folder = Path(self._own_folder.name if self._own_folder else folder).resolve()
        self._own_db = connection is None
        self._db = duckdb.connect() if connection is None else connection
        if self._own_db:
            _host.run(self._db, {'op': 'fileSearchPath', 'folder': str(self._folder)})
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._serve, name='legend-lite-datacube', daemon=True)
        self._thread.start()
        self._stack = contextlib.AsyncExitStack()
        self._mr: Any = None
        self._memory_limit = memory_limit
        # one call into the context at a time: taken on the caller's thread, before anything the call holds (``turn``)
        self._turn = threading.RLock()
        self._reading = reading
        # the read the call in its turn was made in (Frames.reading), if any: what its queries and its asks of the engine
        # read through
        self._over: Any = None
        # the engine's own answers to a call of DataCube's (``engine``: a grid's query planned and run where its rows are,
        # a warehouse table opened under the session it holds), over the call's read, given by the engine's owner as a
        # weak reference to them (weakref.WeakMethod): this engine never keeps its owner alive; none, or its owner gone:
        # no such call answered
        self.answers: Callable[[], Callable[[str, str, Any], tuple[int, str, bytes]] | None] | None = None
        self.broken = False
        try:
            self._run(self._open(bundle, memory_limit))
        except BaseException:
            self.close()
            raise

    def _serve(self) -> None:
        # the engine's thread calls the native library (the planner's requests, the engine's answers): one of the
        # compiler's threads, so a call it makes through the pool runs here, never waiting on the pool's
        library_thread()
        self._loop.run_forever()

    @contextlib.contextmanager
    def turn(self) -> Iterator[None]:
        """This thread's turn at the engine: its calls inside are made one after another, no other thread's between them.
        A caller that holds a lock while it calls (the frames', writing their database through DataCube's code) takes its
        turn first, so no thread holds that lock while it waits for a turn."""
        with self._turn:
            yield

    @property
    def folder(self) -> Path:
        """The engine's own folder: where the files DataCube registers are written, and where its host writes a file
        for DataCube's code to read (a page's file: the engine's file.json)."""
        return self._folder

    # ---- the calls out of V8, each on the engine's loop

    async def _planner(self, request: str) -> str:
        return json.dumps(answer(self._lib, json.loads(request)))

    async def _query(self, sql: str) -> str:
        import pyarrow as pa

        # a call made in a read of the frames queries through it: the database as it was when the read began
        result = self._db.execute(sql) if self._over is None else self._over.execute(sql)
        table = result.to_arrow_table() if result.description is not None else pa.table({})
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return base64.b64encode(sink.getvalue()).decode('ascii')

    async def _register_file(self, name: str, data: str) -> None:
        root = self._folder
        target = (root / name).resolve()
        if root not in target.parents:
            raise ValueError(f'a registered file stays in the engine\'s folder: {name!r} does not')
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(base64.b64decode(data))

    async def _fetch(self, method: str, url: str, body: str | None) -> str:
        status, text = answer_fetch(method, url, body)
        return json.dumps({'status': status, 'body': text})

    async def _engine(self, path: str, body: str) -> str:
        """One call of DataCube's to the engine it runs beside (calls.ts ``engine``: ``/plan-and-run.json``,
        ``/warehouse.json``), answered by the engine's own code here, on the engine's thread, over the read the call was
        made in (D15) -- what was served when it began, read through its own cursor -- so the answer waits on no lock and
        no other thread. A call made in no read asks nothing of the engine: refused, in words. ``{status, contentType,
        body}`` (the body base64), as JSON."""
        answers = None if self.answers is None else self.answers()
        if answers is None:
            raise RuntimeError('this host answers no call of the engine\'s: it was given none to answer')
        if self._over is None:
            raise RuntimeError('DataCube asked the engine in a call made in no read of the frames (Frames.reading): '
                               'its rows are read over what one read sees')
        status, content_type, data = answers(path, body, self._over)
        return json.dumps({'status': status, 'contentType': content_type, 'body': base64.b64encode(data).decode('ascii')})

    async def _digest(self, algorithm: str, data: str) -> str:
        name = _DIGESTS.get(str(algorithm).upper())
        if name is None:
            raise ValueError(f'crypto.subtle.digest: {algorithm!r} is not a digest WebCrypto names ({sorted(_DIGESTS)})')
        return base64.b64encode(hashlib.new(name, base64.b64decode(data)).digest()).decode('ascii')

    # ---- the engine's life

    async def _open(self, bundle: str, memory_limit: int) -> None:
        from py_mini_racer import MiniRacer

        # made on the engine's loop, so mini-racer runs on it (and never on a caller's)
        self._mr = MiniRacer()
        self._mr.set_hard_memory_limit(memory_limit)
        # no WebAssembly in this process (the user's ruling, 2026-10-11): anything that reached for it fails here, loudly
        self._mr.eval('delete globalThis.WebAssembly;')
        # mini-racer 0.14.1's setTimeout waits on Atomics.waitAsync, which with a delay of 0 or less answers at once,
        # without a promise, so the timer throws, and with no delay waits forever (NaN): HTML's setTimeout takes both as
        # 0 (https://github.com/bpcreech/PyMiniRacer/issues/97). Each delay held to at least a millisecond, a real timer
        # on V8's own loop (a browser's own fire after 0 to 4 ms). This goes when the pin (tools/python/requirements.in)
        # moves past mini-racer's fix: //datacube:headless_test's timer test says whether it still has to be here
        self._mr.eval('{ const engines = setTimeout;'
                      ' globalThis.setTimeout = (f, ms, ...args) => engines(f, Math.max(1, Number(ms) || 0), ...args); }')
        # the host's calls first, as a browser's globals are there before a page's scripts: a module may plan as it loads
        calls = [await self._stack.enter_async_context(self._mr.wrap_py_function(f))
                 for f in (self._planner, self._query, self._register_file, self._digest, self._fetch, self._engine)]
        define = self._mr.eval('(planner, query, registerFile, digest, fetch, engine) => {'
                               ' globalThis.legendLiteHost = { planner, query, registerFile, digest, fetch, engine }; }')
        define(*calls)
        self._mr.eval(bundle)
        # a bundle whose modules await as they load (a test bundle's fixtures) says when it has: legendLiteReady
        await self._mr.eval('Promise.resolve(globalThis.legendLiteReady)')
        check_interface(json.loads(self._mr.eval('JSON.stringify(legendLiteHeadless.REQUEST_KINDS)')), self._lib)
        self._mr.eval('globalThis.legendLite = legendLiteHeadless.start();')

    def _run(self, coroutine: Coroutine[Any, Any, Any], timeout: float | None = None) -> Any:
        """``coroutine`` run on the engine's loop, waited for."""
        if threading.current_thread() is self._thread:
            coroutine.close()
            raise RuntimeError("the engine's own thread cannot wait for the engine")
        if self.broken:
            coroutine.close()
            raise RuntimeError('this engine ran out of its memory and is closed to calls: its owner makes another')
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout)
        except TimeoutError:
            future.cancel()
            raise TimeoutError(f'DataCube did not answer within {timeout} s') from None

    async def _evaluate(self, expression: str, over: Any) -> Any:
        # the read the call was made in, while it runs: what its queries and its asks of the engine read through
        self._over = over
        try:
            promise = self._mr.eval(f'Promise.resolve({expression}).then((v) => JSON.stringify(v === undefined ? null : v))')
            return json.loads(await promise)
        except Exception as e:
            if self._mr.was_hard_memory_limit_reached():
                # V8 stopped this call at the cap; what its context holds now is not to be trusted with another
                self.broken = True
                raise MemoryError(f"DataCube's engine reached its memory cap ({self._memory_limit} bytes)") from e
            raise
        finally:
            self._over = None

    def evaluate(self, expression: str, timeout: float | None = None) -> Any:
        """A JavaScript expression's value, awaited when it is a Promise, as JSON gives it to Python: within ``timeout``
        seconds when given (it then raises TimeoutError), else as long as it takes. Made in this thread's turn, over the
        read of the frames this thread is in (``reading``), if any."""
        self._not_its_own()
        over = None if self._reading is None else self._reading()
        with self._turn:
            return self._run(self._evaluate(expression, over), timeout)

    def _not_its_own(self) -> None:
        # before the turn is asked for: the engine's thread asking would wait on its own caller's turn for ever (F38)
        if threading.current_thread() is self._thread:
            raise RuntimeError("the engine's own thread cannot wait for the engine")

    def call(self, function: str, *args: Any, timeout: float | None = None) -> Any:
        """One of the bundle's functions (``legendLiteHeadless.<function>``) called with ``args`` as JSON gives them,
        awaited; its answer as JSON gives it to Python."""
        return self.evaluate(f'legendLiteHeadless.{function}(...{json.dumps(list(args))})', timeout)

    def execute(self, code: str) -> None:
        """JavaScript code run in the engine's context, for its effects."""
        async def run() -> None:
            self._mr.eval(code)
        self._not_its_own()
        with self._turn:
            self._run(run())

    def close(self) -> None:
        """Ends the engine: its calls let go, V8's context, the loop's thread, the database, and its own folder."""
        if self._loop.is_running():
            async def shut() -> None:
                await self._stack.aclose()
                if self._mr is not None:
                    self._mr.close()
                    self._mr = None
            with contextlib.suppress(Exception):
                asyncio.run_coroutine_threadsafe(shut(), self._loop).result()
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
        self._loop.close()
        if self._own_db:
            self._db.close()
        if self._own_folder is not None:
            self._own_folder.cleanup()

    def __enter__(self) -> Headless:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


# the product's bundle: beside this module in the wheel (the `datacube` extra, step 4), or where LEGEND_LITE_DATACUBE says
_BUNDLE = Path(__file__).with_name('_datacube') / 'legend-lite-headless.js'


def bundle() -> str:
    """DataCube's headless bundle, as legend-lite ships it (or LEGEND_LITE_DATACUBE names it)."""
    import os

    path = Path(os.environ.get('LEGEND_LITE_DATACUBE') or _BUNDLE)
    if not path.is_file():
        raise OSError(f"DataCube's code is not at {path}, where the legend-lite wheel carries it: a legend-lite run from "
                      "its sources names the bundle it built in LEGEND_LITE_DATACUBE")
    return path.read_text(encoding='utf-8')


def cells(value: Any) -> Any:
    """A grid's JSON read back exactly: the bundle tags an integer past 2^53 ({"$int": ...}) and a float JSON cannot
    write ({"$float": "NaN"}, "Infinity", "-Infinity", "-0.0")."""
    if isinstance(value, dict):
        if value.keys() == {'$int'}:
            return int(value['$int'])
        if value.keys() == {'$float'}:
            return float(value['$float'])
        return {k: cells(v) for k, v in value.items()}
    if isinstance(value, list):
        return [cells(v) for v in value]
    return value
