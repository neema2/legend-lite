"""A small Legend engine over frames: legend-engine's ``pure/v1`` API, answered on this machine, so DataCube's
remote client (``engine-client/src/engine-remote.ts``) runs against a Python process exactly as it runs against
legend-engine. Design: ``docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md``.

    frames = Frames()
    frames.register('trades', df)
    engine = Engine(frames)          # serving at engine.url, in a background thread
    ...
    engine.close()

Every answer but execute's rows is legend-lite's own server code (``PureV1Api``, through the native library),
so this module holds no Legend protocol: it carries HTTP, and it runs the SQL the compiler wrote. execute
answers in upstream's Arrow format (``?serializationFormat=ARROW_IPC``: a zstd-compressed Arrow IPC stream
whose schema metadata carries the builder, the SQL that ran and the columns), the one format this engine
declares; DuckDB produces Arrow, so JSON would be the extra conversion.

Only what the developer's own page sends is answered: the engine listens on 127.0.0.1 alone, every API call
carries the engine's one-time token (``Authorization: Bearer <token>``), a request is refused unless its Host
names this machine (DNS rebinding), and no cross-origin header is ever sent. Only queries over the models
the frames' tables were written with are run, and only SQL the compiler wrote.
"""

from __future__ import annotations

import contextlib
import hmac
import http.server
import json
import secrets
import socket
import socketserver
import threading
import traceback
import queue
from concurrent.futures import Future
from typing import Any
from pathlib import Path
from urllib.parse import SplitResult, parse_qs, unquote, urlsplit

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


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """An HTTP server, a thread per connection, its answers computed on the shared pool. Closing it closes its open
    connections and waits for the requests being answered."""

    allow_reuse_address = False
    daemon_threads = True

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._open: set[socket.socket] = set()
        self._settled = threading.Condition()
        super().__init__(('127.0.0.1', 0), _Handler)

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
                self._settled.notify_all()

    def server_close(self) -> None:
        super().server_close()
        with self._settled:
            for connection in self._open:
                # a request still being read ends now; one being answered finishes its answer first
                with contextlib.suppress(OSError):
                    connection.shutdown(socket.SHUT_RD)
            self._settled.wait_for(lambda: not self._open)


class _Handler(http.server.BaseHTTPRequestHandler):
    # HTTP/1.0 (the stdlib's default): each answer closes its connection; one that never sends is closed after _IDLE
    # seconds
    server: _Server
    timeout = _IDLE

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 -- the stdlib's name
        pass

    def _send(self, status: int, content_type: str, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _refuse(self, status: int, message: str) -> None:
        self._send(status, 'text/plain; charset=utf-8', message.encode('utf-8'))

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
        self._refuse(status, message)

    def _admitted(self) -> bool:
        """The request names this machine and carries the token; otherwise it is refused, and False."""
        engine = self.server.engine
        if self.headers.get('Host') not in engine._hosts:
            self._refuse_unread(403, 'this engine answers requests to 127.0.0.1 and localhost only')
            return False
        given = self.headers.get('Authorization', '')
        if not hmac.compare_digest(given.encode('utf-8'), engine._authorization):
            self._refuse_unread(401, "this engine answers its own page only: the request did not carry the engine's token")
            return False
        return True

    def do_POST(self) -> None:  # noqa: N802 -- the stdlib's name
        url = urlsplit(self.path)
        if not url.path.startswith(_API):
            self._refuse_unread(404, f'no such path: {url.path}')
            return
        if not self._admitted():
            return
        length = self._length()
        if length is None:
            self._refuse(411, 'a request body needs its Content-Length, in digits')
            return
        if length > _MAX_BODY:
            self._refuse(413, f'a request body is at most {_MAX_BODY} bytes')
            return
        try:
            raw = self.rfile.read(length)
        except OSError:
            # the client stopped sending, or went: there is no one to answer
            return
        if len(raw) < length:
            # the connection ended before its body did (the client went, or the engine is closing)
            return
        try:
            body = raw.decode('utf-8')
        except UnicodeDecodeError:
            self._refuse(400, 'a request body is UTF-8')
            return
        self._send(*_workers().submit(self._answer, url, body).result())

    def _answer(self, url: SplitResult, body: str) -> tuple[int, str, bytes, dict[str, str]]:
        """The answer to an admitted call, computed on the compiler's pool."""
        try:
            if url.path == _EXECUTE and _arrow_requested(url.query):
                return self._execute(body)
            answer = compiler.pure_v1(url.path, url.query, body)
        except Exception as failure:
            # never a dropped connection: the database refusing is an answer; anything else is a bug, logged whole,
            # and answered the same way (as legend-lite's server answers both)
            if not isinstance(failure, duckdb.Error):
                traceback.print_exc()
            message = f'{type(failure).__name__}: {failure}'
            try:
                answer = compiler.refusal(message)
            except Exception:
                # the compiler itself failing: still an answer, its own words
                traceback.print_exc()
                return 500, 'text/plain; charset=utf-8', message.encode('utf-8'), {}
        return answer.status, answer.content_type, answer.body.encode('utf-8'), {}

    def _execute(self, body: str) -> tuple[int, str, bytes, dict[str, str]]:
        """execute in upstream's Arrow format: the compiler plans over the frames' models, DuckDB runs its SQL,
        and the rows go back as upstream writes them. One query at a time (the frames are held still while it
        runs); the answer is compressed outside."""
        frames = self.server.engine.frames
        with frames.serving() as models:
            self.server.engine._noted(models)
            answer = compiler.execute_plan(body, list(models.values()))
            if answer.status != 200:
                return answer.status, answer.content_type, answer.body.encode('utf-8'), {}
            planned = json.loads(answer.body)
            rows = frames.connection.execute(planned['sql']).to_arrow_table()
        # upstream's headers for this answer (measured, legend-engine 4.145.0)
        return 200, 'application/json', _arrow_ipc(rows, planned['metadata']), {'x-legend-response-format': 'FormatNotSet'}

    def do_GET(self) -> None:  # noqa: N802
        """A file of the engine's site (DataCube's built pages, given as ``site``), to a Host naming this machine;
        the files are the page's own and carry no secret, so no token is asked (the page holds it)."""
        url = urlsplit(self.path)
        path = url.path
        site = self.server.engine.site
        if path == '/cube.json':
            # what a page shows: asked with the token, as every API call is
            if self._admitted():
                self._send(*_workers().submit(self._cube, url.query).result())
            return
        if path == '/version.json':
            # a page asking whether its frame changed (no compiler call: on this connection's own thread)
            if self._admitted():
                self._send(*self._version(url.query))
            return
        if self.headers.get('Host') not in self.server.engine._hosts:
            self._refuse_unread(403, 'this engine answers requests to 127.0.0.1 and localhost only')
            return
        relative = 'index.html' if path == '/' else unquote(path).lstrip('/')
        parts = relative.split('/')
        file = site / relative if site is not None else None
        # the path as written, never resolved: a built site's files are links into the build's output (Bazel's), as
        # the warehouse serves them (WarehouseServer.site)
        # a part holding ':' would be a drive on Windows ('C:/x' replaces the root), one holding NUL no file name
        if (file is None or '\\' in relative or '..' in parts or '' in parts
                or any(':' in part or '\0' in part for part in parts) or not file.is_file()):
            self._refuse_unread(404, f'no such path: {path}')
            return
        self._send(200, _SITE_TYPES.get(file.suffix, 'application/octet-stream'), file.read_bytes(), {})

    def _cube(self, query: str) -> tuple[int, str, bytes, dict[str, str]]:
        """The cube a page of the site shows (DataCube's demo/engine.html): ``table`` (a frame's name), its model,
        runtime and source as they are now (a Live frame read again), and its title. Computed on the compiler's
        pool."""
        name = parse_qs(query).get('table', [''])[0]
        frames = self.server.engine.frames
        try:
            with frames.serving() as models:
                self.server.engine._noted(models)
                if name not in frames:
                    return 404, 'text/plain; charset=utf-8', f'this engine serves no frame named {name!r}'.encode(), {}
                table = frames[name]
                cube = {'title': table.name, 'model': table.model, 'runtime': table.runtime, 'source': table.source,
                        'version': self.server.engine.version(name)}
            body = json.dumps(cube).encode('utf-8')
        except Exception as failure:
            # a frame that could not be read: said, never a dropped connection
            traceback.print_exc()
            return 500, 'text/plain; charset=utf-8', f'{type(failure).__name__}: {failure}'.encode(), {}
        return 200, 'application/json', body, {}

    def _version(self, query: str) -> tuple[int, str, bytes, dict[str, str]]:
        """``table``'s version (``Engine.changed``): ``{"version": n}`` at once, or 404 when the engine serves no such
        frame (it was closed). A page asks about once a second; nothing is held open while it waits, so cubes in many
        tabs never use up the browser's six connections to one origin."""
        name = parse_qs(query).get('table', [''])[0]
        engine = self.server.engine
        if name not in engine.frames:
            return 404, 'text/plain; charset=utf-8', f'this engine serves no frame named {name!r}'.encode(), {}
        return 200, 'application/json', json.dumps({'version': engine.version(name)}).encode('utf-8'), {}

    def do_OPTIONS(self) -> None:  # noqa: N802
        # no cross-origin request is answered: only the engine's own page calls it
        self._refuse_unread(405, 'this engine answers no cross-origin request')


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
    """legend-engine's ``pure/v1`` API over the frames' tables, served at ``url`` from a background thread until
    ``close()``. Requests carry ``authorization`` (the token). ``site``: a directory whose files are served too
    (DataCube's built pages), at the same origin, so the page needs no cross-origin access."""

    def __init__(self, frames: Frames, site: str | Path | None = None) -> None:
        self.frames = frames
        self.site = Path(site).resolve() if site is not None else None
        if self.site is not None and not self.site.is_dir():
            raise ValueError(f'the site {site} is not a directory')
        self.token = secrets.token_urlsafe(32)
        self._authorization = f'Bearer {self.token}'.encode('utf-8')
        # each frame's version: bumped when it changes (changed), so a page asking sees it and reads it again; and the
        # model each was last served with, so a Live frame whose columns changed is noticed (_noted)
        self._versions: dict[str, int] = {}
        self._models: dict[str, str] = {}
        self._changes = threading.Lock()
        self._server = _Server(self)
        port = self._server.server_address[1]
        self.url = f'http://127.0.0.1:{port}'
        self._hosts = frozenset({f'127.0.0.1:{port}', f'localhost:{port}'})
        self._thread = threading.Thread(target=self._server.serve_forever, name='legend-lite-engine', daemon=True)
        self._thread.start()

    @property
    def authorization(self) -> str:
        """The header value every request carries: ``Bearer <token>``."""
        return self._authorization.decode('utf-8')

    def changed(self, name: str) -> None:
        """Says a frame changed -- its rows, its columns (registered again), or that it was closed -- so a page
        showing it reads it again: one with the same columns re-runs its view, one with new columns opens again over
        the new model, one whose frame is gone stops following it."""
        with self._changes:
            key = name.lower()
            self._versions[key] = self._versions.get(key, 0) + 1

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

    def close(self) -> None:
        """Stops serving; requests being answered finish first."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def __enter__(self) -> Engine:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
