"""A small Legend engine over frames: legend-engine's ``pure/v1`` API, answered on this machine, so DataCube's
remote client (``engine-client/src/engine-remote.ts``) runs against a Python process exactly as it runs against
legend-engine. Design: ``docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md``.

    frames = Frames()
    frames.register('trades', df)
    engine = Engine(frames)              # the answers: engine.answer(method, path, query, body)
    server = WebServer(engine)           # to a browser tab: HTTP at server.url, in a background thread
    ...
    server.close()

The engine has no transport. A browser tab reaches it over HTTP (``WebServer``); a notebook's cube over the widget's
own channel (``legend_lite.notebook``). Both send it the same calls.

Every answer but execute's rows is legend-lite's own server code (``PureV1Api``, through the native library),
so this module holds no Legend protocol: it runs the SQL the compiler wrote, and carries HTTP. execute
answers in upstream's Arrow format (``?serializationFormat=ARROW_IPC``: a zstd-compressed Arrow IPC stream
whose schema metadata carries the builder, the SQL that ran and the columns), the one format this engine
declares; DuckDB produces Arrow, so JSON would be the extra conversion.

Over HTTP, only what the developer's own page sends is answered: the server listens on 127.0.0.1 alone, every call
for the engine's data carries its one-time token (``Authorization: Bearer <token>``), a request is refused unless its
Host names this machine (DNS rebinding), and no cross-origin header is ever sent. Only queries over the models the
frames' tables were written with are run, and only SQL the compiler wrote.
"""

from __future__ import annotations

import contextlib
import hmac
import http.server
import json
import secrets
import socket
import socketserver
import sys
import threading
import traceback
import queue
from concurrent.futures import Future
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any, NamedTuple
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import duckdb
import pyarrow as pa
import pyarrow.ipc

from . import compiler
from .frames import Frames

_API = '/api/pure/v1/'
_EXECUTE = _API + 'execution/execute'
# a request body larger than this is refused: a query and its model are kilobytes
_MAX_BODY = 16 * 1024 * 1024
# a connection that sends nothing, or stops sending, is closed after this long (seconds)
_IDLE = 30
# the threads that call the compiler: ONE fixed pool for every engine in the process, as the native library asks of
# a host (each thread that calls it stays attached to its isolate), so opening engine after engine in a notebook
# attaches no new threads. A connection is read and written on a thread of its own, which never calls the compiler:
# an idle connection holds no compiler thread
_WORKERS = 4


class _Pool:
    """The compiler's threads: daemon threads taking calls from one queue. Not concurrent.futures' pool: Python shuts
    that down before its atexit handlers run, and a plain script's end waits in one (show(): the cubes stay open until
    Ctrl-C), so every call the engine made there would be refused (the audit of show(), 2026-10-08)."""

    def __init__(self, size: int) -> None:
        self._calls: queue.SimpleQueue[tuple[Future[Any], Any, tuple[Any, ...]]] = queue.SimpleQueue()
        for i in range(size):
            threading.Thread(target=self._serve, name=f'legend-lite-compiler-{i}', daemon=True).start()

    def submit(self, call: Any, *args: Any) -> Future[Any]:
        future: Future[Any] = Future()
        self._calls.put((future, call, args))
        return future

    def _serve(self) -> None:
        while True:
            future, call, args = self._calls.get()
            if not future.set_running_or_notify_cancel():
                continue
            try:
                future.set_result(call(*args))
            except BaseException as failure:  # noqa: BLE001 -- handed to the caller, which answers it
                future.set_exception(failure)


_pool: _Pool | None = None
_pool_lock = threading.Lock()


def _workers() -> _Pool:
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = _Pool(_WORKERS)
        return _pool


# the site's media types (the warehouse's, WarehouseServer.SITE_TYPES)
_SITE_TYPES = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.mjs': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json',
    '.wasm': 'application/wasm',
    '.pure': 'text/plain; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.woff2': 'font/woff2',
}

_NO_HEADERS: Mapping[str, str] = MappingProxyType({})


class Answer(NamedTuple):
    """An answer to one call, as HTTP would carry it: its status, its media type, its body and any other headers."""

    status: int
    content_type: str
    body: bytes
    headers: Mapping[str, str] = _NO_HEADERS


def _said(status: int, message: str) -> Answer:
    return Answer(status, 'text/plain; charset=utf-8', message.encode('utf-8'))


def private(path: str) -> bool:
    """Whether a call reads the engine's data -- the API, a frame's cube, its version -- and so, over HTTP, carries the
    token. The site's files are the page's own and carry no secret."""
    return path.startswith(_API) or path in ('/cube.json', '/version.json', '/page.json')


def _arrow_requested(query: str) -> bool:
    return any(pair == 'serializationFormat=ARROW_IPC' for pair in query.split('&'))


def _arrow_ipc(rows: pa.Table, metadata: dict[str, str]) -> bytes:
    """Rows as upstream's ARROW_IPC answer: an Arrow IPC stream, its schema carrying Legend's metadata, the whole
    stream compressed as one zstd frame."""
    declared = json.loads(metadata['legend.columns'])
    if rows.column_names != declared:
        raise RuntimeError(f'the database answered columns {rows.column_names}, the plan declared {declared}')
    rows = rows.replace_schema_metadata(metadata)
    sink = pa.BufferOutputStream()
    with pa.CompressedOutputStream(sink, 'zstd') as compressed:
        with pyarrow.ipc.new_stream(compressed, rows.schema) as writer:
            writer.write_table(rows)
    return sink.getvalue().to_pybytes()


class Engine:
    """legend-engine's ``pure/v1`` API over the frames' tables, with no transport: ``answer(method, path, query,
    body)`` is one call, answered as HTTP would carry it. ``site``: a directory whose files are answered too
    (DataCube's built pages). ``WebServer`` serves it to a browser tab; a notebook's cube sends it its widget's
    messages."""

    def __init__(self, frames: Frames, site: str | Path | None = None) -> None:
        self.frames = frames
        self.site = Path(site).resolve() if site is not None else None
        if self.site is not None and not self.site.is_dir():
            raise ValueError(f'the site {site} is not a directory')
        # each frame's version: bumped when it changes (changed), so a page asking sees it and reads it again; and the
        # model each was last served with, so a Live frame whose columns changed is noticed (_noted)
        self._versions: dict[str, int] = {}
        self._models: dict[str, str] = {}
        self._watchers: list[Callable[[str, int], None]] = []
        self._changes = threading.Lock()
        # the pages it serves (Python's ll.Page; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md): each its document --
        # DataCube's page document, its cubes over these frames -- and its version, moved each time it is served again
        self._pages: dict[str, tuple[int, dict[str, Any]]] = {}
        # each page's document as the open page says it is now (its changes made in DataCube included: page.read())
        self._read: dict[str, dict[str, Any]] = {}

    def answer(self, method: str, path: str, query: str, body: str | None) -> Answer:
        """One call: ``POST`` to the API, or ``GET`` of a frame's cube (``cube.json``), its version (``version.json``) or
        a file of the site. It waits for its answer, which the compiler's threads compute when it needs the compiler,
        so it is called from a thread of the caller's own (a connection's, a message's), never a compiler thread."""
        if method == 'POST':
            if path == '/page.json':
                return self._page_said(query, body or '')
            if not path.startswith(_API):
                return _said(404, f'no such path: {path}')
            return _workers().submit(self._api, path, query, body or '').result()
        if method != 'GET':
            return _said(405, 'this engine answers GET and POST')
        if path == '/cube.json':
            return _workers().submit(self._cube, query).result()
        if path == '/version.json':
            # no compiler call: on the caller's thread
            return self._version(query)
        if path == '/page.json':
            return self._page(query)
        return self._file(path)

    def _api(self, path: str, query: str, body: str) -> Answer:
        """An API call's answer, computed on the compiler's pool."""
        try:
            if path == _EXECUTE and _arrow_requested(query):
                return self._execute(body)
            answer = compiler.pure_v1(path, query, body)
        except Exception as failure:
            # never a dropped call: the database refusing is an answer; anything else is a bug, logged whole, and
            # answered the same way (as legend-lite's server answers both)
            if not isinstance(failure, duckdb.Error):
                traceback.print_exc()
            message = f'{type(failure).__name__}: {failure}'
            try:
                answer = compiler.refusal(message)
            except Exception:
                # the compiler itself failing: still an answer, its own words
                traceback.print_exc()
                return _said(500, message)
        return Answer(answer.status, answer.content_type, answer.body.encode('utf-8'))

    def _execute(self, body: str) -> Answer:
        """execute in upstream's Arrow format: the compiler plans over the frames' models, DuckDB runs its SQL,
        and the rows go back as upstream writes them. One query at a time (the frames are held still while it
        runs); the answer is compressed outside."""
        with self.frames.serving() as models:
            self._noted(models)
            answer = compiler.execute_plan(body, list(models.values()))
            if answer.status != 200:
                return Answer(answer.status, answer.content_type, answer.body.encode('utf-8'))
            planned = json.loads(answer.body)
            rows = self.frames.connection.execute(planned['sql']).to_arrow_table()
        # upstream's headers for this answer (measured, legend-engine 4.145.0)
        return Answer(200, 'application/json', _arrow_ipc(rows, planned['metadata']),
                      {'x-legend-response-format': 'FormatNotSet'})

    def _cube(self, query: str) -> Answer:
        """The cube a page shows (DataCube's demo/engine.html, or a notebook's widget): ``table`` (a frame's name), its
        model, runtime and source as they are now (a Live frame read again), and its title. Computed on the compiler's
        pool."""
        name = parse_qs(query).get('table', [''])[0]
        try:
            with self.frames.serving() as models:
                self._noted(models)
                if name not in self.frames:
                    return _said(404, f'this engine serves no frame named {name!r}')
                table = self.frames[name]
                cube = {'title': table.name, 'model': table.model, 'runtime': table.runtime, 'source': table.source,
                        'version': self.version(name)}
            body = json.dumps(cube).encode('utf-8')
        except Exception as failure:
            # a frame that could not be read: said, never a dropped call
            traceback.print_exc()
            return _said(500, f'{type(failure).__name__}: {failure}')
        return Answer(200, 'application/json', body)

    def _version(self, query: str) -> Answer:
        """``table``'s version (``changed``): ``{"version": n}`` at once, or 404 when the engine serves no such frame
        (it was closed). A tab asks about once a second; nothing is held open while it waits, so cubes in many tabs
        never use up the browser's six connections to one origin. Asked with ``page``, a page's: ``{"version": n,
        "frames": {name: n}}``, the page's own and each of its frames' (``page_versions``)."""
        asked = parse_qs(query)
        if 'page' in asked:
            versions = self.page_versions(asked['page'][0])
            if versions is None:
                return _said(404, f'this engine serves no page named {asked["page"][0]!r}')
            return Answer(200, 'application/json', json.dumps(versions).encode('utf-8'))
        name = asked.get('table', [''])[0]
        if name not in self.frames:
            return _said(404, f'this engine serves no frame named {name!r}')
        return Answer(200, 'application/json', json.dumps({'version': self.version(name)}).encode('utf-8'))

    def _page(self, query: str) -> Answer:
        """A page it serves (``serve_page``): ``{"version": n, "page": <its document>}``, or 404."""
        name = parse_qs(query).get('page', [''])[0]
        with self._changes:
            served = self._pages.get(name)
        if served is None:
            return _said(404, f'this engine serves no page named {name!r}')
        return Answer(200, 'application/json', json.dumps({'version': served[0], 'page': served[1]}).encode('utf-8'))

    def _page_said(self, query: str, body: str) -> Answer:
        """The open page's document as it is now (posted by the page as it changes): kept for ``read_page``."""
        name = parse_qs(query).get('page', [''])[0]
        with self._changes:
            if name not in self._pages:
                return _said(404, f'this engine serves no page named {name!r}')
        try:
            document = json.loads(body)
        except ValueError:
            return _said(400, 'a page is its document, as JSON')
        if not isinstance(document, dict) or document.get('kind') != 'datacube.page':
            return _said(400, 'a page is its document (kind datacube.page)')
        with self._changes:
            self._read[name] = document
        return Answer(204, 'text/plain; charset=utf-8', b'')

    def read_page(self, name: str) -> dict[str, Any] | None:
        """A page's document as the open page last said it is (its changes made in DataCube included), or None when it
        has said nothing since it was served."""
        with self._changes:
            return self._read.get(name)

    def serve_page(self, name: str, document: dict[str, Any]) -> int:
        """Serves ``document`` -- DataCube's page document (version 3), each cube over one of this engine's frames
        (``{"_type": "frame", "name": ...}``) -- as the page ``name``; served again, its version moves, and a page open
        on it opens it again. Returns its version."""
        with self._changes:
            version = self._pages.get(name, (0, {}))[0] + 1
            self._pages[name] = (version, document)
            # what the open page said is of the document before: it opens this one
            self._read.pop(name, None)
        return version

    def close_page(self, name: str) -> None:
        """Stops serving a page: one open on it stops following it."""
        with self._changes:
            self._pages.pop(name, None)
            self._read.pop(name, None)

    def page_versions(self, name: str) -> dict[str, Any] | None:
        """A page's version, and each of its frames' (the frames its cubes read), or None when it serves no such page."""
        with self._changes:
            served = self._pages.get(name)
        if served is None:
            return None
        frames = sorted({cube['cube']['source']['name'] for cube in served[1].get('cubes', [])
                         if cube.get('cube', {}).get('source', {}).get('_type') == 'frame'})
        return {'version': served[0], 'frames': {frame: self.version(frame) for frame in frames}}

    def _file(self, path: str) -> Answer:
        """A file of the site, by its path under it."""
        relative = 'index.html' if path == '/' else unquote(path).lstrip('/')
        parts = relative.split('/')
        file = self.site / relative if self.site is not None else None
        # the path as written, never resolved: a built site's files are links into the build's output (Bazel's), as
        # the warehouse serves them (WarehouseServer.site)
        # a part holding ':' would be a drive on Windows ('C:/x' replaces the root), one holding NUL no file name
        if (file is None or '\\' in relative or '..' in parts or '' in parts
                or any(':' in part or '\0' in part for part in parts) or not file.is_file()):
            return _said(404, f'no such path: {path}')
        return Answer(200, _SITE_TYPES.get(file.suffix, 'application/octet-stream'), file.read_bytes())

    def changed(self, name: str) -> None:
        """Says a frame changed -- its rows, its columns (registered again), or that it was closed -- so a page
        showing it reads it again: one with the same columns re-runs its view, one with new columns opens again over
        the new model, one whose frame is gone stops following it."""
        with self._changes:
            key = name.lower()
            version = self._versions[key] = self._versions.get(key, 0) + 1
            watchers = list(self._watchers)
        for watcher in watchers:
            try:
                watcher(name, version)
            except Exception:
                # one watcher failing is its own bug: the frame's other pages are still told
                traceback.print_exc()

    def watch(self, watcher: Callable[[str, int], None]) -> Callable[[], None]:
        """Calls ``watcher(name, version)`` after each change (``changed``), from the thread that made it; returns what
        stops it. A notebook's cube is told this way; a tab asks (``version.json``)."""
        with self._changes:
            self._watchers.append(watcher)

        def unwatch() -> None:
            with self._changes:
                if watcher in self._watchers:
                    self._watchers.remove(watcher)
        return unwatch

    def seen(self, name: str) -> bool:
        """Whether this engine has served a frame of this name (now or before it was closed)."""
        with self._changes:
            return name.lower() in self._versions or name.lower() in self._models

    def _noted(self, models: dict[str, str]) -> None:
        """The frames' models as they are now (each Live one read again): one that differs from what it was last
        served with -- a Live frame's columns changed -- is a change its page is told of."""
        moved = []
        with self._changes:
            for name, model in models.items():
                key = name.lower()
                if key in self._models and self._models[key] != model:
                    moved.append(name)
                self._models[key] = model
        for name in moved:
            self.changed(name)

    def version(self, name: str) -> int:
        """A frame's version: how many times it has changed (``changed``)."""
        with self._changes:
            return self._versions.get(name.lower(), 0)


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """An HTTP server, a thread per connection, its answers computed on the shared pool. Closing it closes its open
    connections and waits for the requests being answered."""

    allow_reuse_address = False
    daemon_threads = True

    def __init__(self, web: WebServer) -> None:
        self.web = web
        self._open: set[socket.socket] = set()
        # the connections whose request was read whole and is being answered: a close lets them finish
        self._answering: set[socket.socket] = set()
        self._closing = False
        self._settled = threading.Condition()
        super().__init__(('127.0.0.1', 0), _Handler)

    def answering(self, connection: socket.socket) -> None:
        with self._settled:
            self._answering.add(connection)

    def process_request(self, request: Any, client_address: Any) -> None:
        with self._settled:
            self._open.add(request)
        super().process_request(request, client_address)

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._settled:
                self._open.discard(request)
                self._answering.discard(request)
                self._settled.notify_all()

    def handle_error(self, request: Any, client_address: Any) -> None:
        # a connection the close cut (below: one still being read) fails its read: that is the close, not a fault. One
        # being answered is reported as ever (_answering is cleared only after this, in process_request_thread)
        if not (self._closing and request not in self._answering):
            super().handle_error(request, client_address)

    def server_close(self) -> None:
        super().server_close()
        with self._settled:
            self._closing = True
            for connection in self._open:
                # a request still being read ends now; one being answered finishes its answer first. A read blocked
                # in another thread ends, on Linux and macOS, when its socket is shut for reading; on Windows a
                # shutdown leaves it blocked (until the idle timeout: 30 s, found by the first Windows run) and only
                # closing the system socket cancels it. connection.close() would not: it puts the real close off until
                # the request's reader (its makefile) is closed, and that reader is the blocked thread -- so the system
                # socket is closed itself (detach, then socket.close of what it held)
                with contextlib.suppress(OSError):
                    if sys.platform != 'win32':
                        connection.shutdown(socket.SHUT_RD)
                    elif connection not in self._answering:
                        socket.close(connection.detach())
            self._settled.wait_for(lambda: not self._open)


class _Handler(http.server.BaseHTTPRequestHandler):
    # HTTP/1.0 (the stdlib's default): each answer closes its connection; one that never sends is closed after _IDLE
    # seconds
    server: _Server
    timeout = _IDLE

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 -- the stdlib's name
        pass

    def _send(self, answer: Answer) -> None:
        self.send_response(answer.status)
        self.send_header('Content-Type', answer.content_type)
        self.send_header('Content-Length', str(len(answer.body)))
        self.send_header('Cache-Control', 'no-store')
        for name, value in answer.headers.items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(answer.body)

    def _length(self) -> int | None:
        """The request's Content-Length, or None when it has none this engine reads (absent, not ASCII digits)."""
        length = self.headers.get('Content-Length')
        if length is None or not (length.isascii() and length.isdigit()):
            return None
        return int(length)

    def _refuse_unread(self, status: int, message: str) -> None:
        """A refusal before the body is wanted: the body is read first (when it is within the limit), so the
        client reads the answer rather than a reset connection."""
        length = self._length()
        if length is not None and length <= _MAX_BODY:
            # in pieces, each dropped: a refused request holds no more than one piece
            with contextlib.suppress(OSError):
                while length > 0:
                    piece = self.rfile.read(min(length, 64 * 1024))
                    if not piece:
                        break
                    length -= len(piece)
        self._send(_said(status, message))

    def _admitted(self, path: str) -> bool:
        """The request names this machine, and carries the token when it reads the engine's data (``private``);
        otherwise it is refused, and False."""
        web = self.server.web
        if self.headers.get('Host') not in web._hosts:
            self._refuse_unread(403, 'this engine answers requests to 127.0.0.1 and localhost only')
            return False
        if private(path):
            given = self.headers.get('Authorization', '')
            if not hmac.compare_digest(given.encode('utf-8'), web._authorization):
                self._refuse_unread(401, "this engine answers its own page only: the request did not carry the engine's token")
                return False
        return True

    def do_POST(self) -> None:  # noqa: N802 -- the stdlib's name
        url = urlsplit(self.path)
        # the API, and an open page saying what it is now (page.json: Python's page.read())
        if not (url.path.startswith(_API) or url.path == '/page.json'):
            self._refuse_unread(404, f'no such path: {url.path}')
            return
        if not self._admitted(url.path):
            return
        length = self._length()
        if length is None:
            self._send(_said(411, 'a request body needs its Content-Length, in digits'))
            return
        if length > _MAX_BODY:
            self._send(_said(413, f'a request body is at most {_MAX_BODY} bytes'))
            return
        try:
            raw = self.rfile.read(length)
        except OSError:
            # the client stopped sending, or went: there is no one to answer
            return
        if len(raw) < length:
            # the connection ended before its body did (the client went, or the server is closing)
            return
        self.server.answering(self.connection)
        try:
            body = raw.decode('utf-8')
        except UnicodeDecodeError:
            self._send(_said(400, 'a request body is UTF-8'))
            return
        self._send(self.server.web.engine.answer('POST', url.path, url.query, body))

    def do_GET(self) -> None:  # noqa: N802
        """A frame's cube or version (with the token), or a file of the engine's site (DataCube's built pages), to a
        Host naming this machine."""
        url = urlsplit(self.path)
        if self._admitted(url.path):
            self.server.answering(self.connection)
            self._send(self.server.web.engine.answer('GET', url.path, url.query, None))

    def do_OPTIONS(self) -> None:  # noqa: N802
        # no cross-origin request is answered: only the engine's own page calls it
        self._refuse_unread(405, 'this engine answers no cross-origin request')


class WebServer:
    """An engine served over HTTP to a browser tab, on 127.0.0.1 at ``url``, from a background thread until
    ``close()``. Calls for the engine's data carry ``authorization`` (the one-time token); the site's files are served
    at the same origin, so the page needs no cross-origin access."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.token = secrets.token_urlsafe(32)
        self._authorization = f'Bearer {self.token}'.encode('utf-8')
        self._server = _Server(self)
        port = self._server.server_address[1]
        self.url = f'http://127.0.0.1:{port}'
        self._hosts = frozenset({f'127.0.0.1:{port}', f'localhost:{port}'})
        self._thread = threading.Thread(target=self._server.serve_forever, name='legend-lite-engine', daemon=True)
        self._thread.start()

    @property
    def authorization(self) -> str:
        """The header value every call for the engine's data carries: ``Bearer <token>``."""
        return self._authorization.decode('utf-8')

    def close(self) -> None:
        """Stops serving; requests being answered finish first."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def __enter__(self) -> WebServer:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
