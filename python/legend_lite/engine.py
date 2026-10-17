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

import base64
import binascii
import contextlib
import functools
import hmac
import http.server
import json
import os
import secrets
import socket
import socketserver
import sys
import threading
import traceback
from collections.abc import Callable, Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any, NamedTuple
from pathlib import Path
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urlsplit

from ._extra import require_datacube

# DataCube's: the datacube extra's packages, or one sentence saying to install it (_extra.py)
require_datacube()

import duckdb
import pyarrow as pa
import pyarrow.ipc

from . import _host, _json, compiler, warehouse
from .compiler import LegendError
from ._library import library
from ._pool import _on_compiler_threads, _workers
from .frames import Frames, UntypedColumn, typed_by_plan
from .projects import ProjectDatabases
from ._home import THIS_USER, Settings, home, model_home_folder, pages_folder, queries_folder, store_call

_API = '/api/pure/v1/'
_EXECUTE = _API + 'execution/execute'
# a request body larger than this is refused: a query and its model are kilobytes, and a page's file comes a part at a
# time (file-part.json: 4 MiB each, as base64 in JSON, datacube/src/upload.ts PART_BYTES), so no file is too big
_MAX_BODY = 16 * 1024 * 1024
# the calls that bring a page's data in and let it go (each changes the engine's data), and what Python's host offers
# the page (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §3.4, §4.3): each the engine's own page's, so each carries the token
_FILE_IN, _FILE_OUT, _REMOTE, _SERVICES = '/file.json', '/forget.json', '/remote.json', '/services.json'
# a page's file's bytes, a part at a time, before file.json reads them
_FILE_PART = '/file-part.json'
# the page's own settings, kept between visits as the browser keeps them (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B2):
# GET what was kept, POST them whole
_SETTINGS = '/settings.json'
# a warehouse table on a page (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3): Python signs in as the person running it,
# lists what they may read, and writes and holds each opened table's model; the token never reaches the page
_WAREHOUSE = '/warehouse.json'
# the most rows a Snap copies into Python's DuckDB: Python's own limit, a fact of the database the copy lands in (this
# machine's), as the tab's (snap.ts MAX_SNAP_ROWS) is of its own (the browser's memory) -- the two need not agree (the
# user and the DataCube + Python line, 2026-10-15)
_MAX_SNAP_ROWS = 10_000_000
# PYTHON'S MODEL HOME (B3): the SDLC's API and the Depot's, as the model home's server mounts them, answered by the
# native library's SDLC over this user's git repository of projects
_SDLC_API, _DEPOT_API = '/sdlc/api/', '/depot/api/'
# legend-engine's query store (/api/pure/v1/query): the saved queries' rules, the server's and the page's, over this
# user's folder (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B5); and who a caller is there, as Python's saved pages name it
_QUERIES, _QUERY_USER = '/api/pure/v1/query', THIS_USER
# DataCube's saved pages, at upstream's routes beside the queries (/api/pure/v1/query/dataCube): the saved pages' rules,
# the server's and the page's, over this user's folder (B6); their caller the same user
_CUBES = _QUERIES + '/dataCube'
# PLANANDRUN (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15): a page's grid planned against the model Python holds and
# run on its DuckDB, in one call; and its dry run, plan-and-run's planning half (the page's compile check) -- the host's
# compute (§15.2 as amended 2026-10-15). A question about the Pure query alone is legend-engine's own route (_API), as
# every Legend server answers it
_PLAN_AND_RUN, _PLAN = '/plan-and-run.json', '/plan.json'
# QUERY AND STUDIO SERVED BY PYTHON (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md E): each app's site at /<app>/ (its page
# /<app>/demo/index.html, its tree as built), on this engine's SDLC, Depot, saved queries and pure/v1; and the cookie the
# link's token is exchanged for on the app's first load, which every call of its page then carries (its clients send
# no token of their own): same-site strict, unreadable by the page's script, and named by the engine's port (a browser
# sends a host's cookies to its every port: two engines' would otherwise replace each other's)
_APPS = ('query', 'studio')
_CURRENT_USER = '/api/server/v1/currentUser'
_LITE_API = '/api/lite/v1/'
# the planner's answer to the plan a page's rows are of, in their Arrow schema's metadata
_PLAN_KEY = b'legend.plan'
# the only places a page's remote file may be read from (remote.json), each as DuckDB's httpfs names it: never this
# machine's own files, which Python's DuckDB can read and a page must not make it (R4's reason). Azure's schemes are not
# here: the wheel carries no azure extension. An Iceberg table's metadata names its files by path, so each of those
# must be one of these too (Engine._iceberg_refusal), or a remote table could make DuckDB read this machine's
_REMOTE_SCHEMES = ('http', 'https', 's3', 's3a', 's3n', 'gs', 'gcs', 'r2', 'hf')
# a connection that sends nothing, or stops sending, is closed after this long (seconds)
_IDLE = 30
def _request_model(body: str) -> str | None:
    """The model text an ExecuteInput carries (a PureModelContextText's code), or None for any other: the compiler
    reads it, and refuses another kind itself (PureV1Api.modelText)."""
    try:
        model = json.loads(body).get('model')
    except (ValueError, AttributeError):
        return None
    if isinstance(model, dict) and model.get('_type') == 'text' and isinstance(model.get('code'), str):
        return model['code']
    return None


@functools.cache
def _site_types() -> dict[str, str]:
    """The site's media types by suffix: the one table every host serving a site answers with (the native library's
    ``lite_site_types``, com.legend.site.SiteTypes: the warehouse's server's too; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md
    C4). Anything else is served as bytes."""
    tag, _, table = library().call('lite_site_types').partition('\n')
    if tag != 'OK':
        raise RuntimeError(f'the native library has no site types: {table}')
    return {f'.{extension}': media for extension, media in json.loads(table).items()}

_NO_HEADERS: Mapping[str, str] = MappingProxyType({})


class Answer(NamedTuple):
    """An answer to one call, as HTTP would carry it: its status, its media type, its body and any other headers."""

    status: int
    content_type: str
    body: bytes
    headers: Mapping[str, str] = _NO_HEADERS


def _said(status: int, message: str) -> Answer:
    return Answer(status, 'text/plain; charset=utf-8', message.encode('utf-8'))


def _arrow_of(chunks: Iterable[bytes]) -> pa.Table:
    """A result's Arrow IPC streams, one per chunk, as one table: the first chunk's schema (every chunk carries it;
    the warehouse's DuckDB writes one at least, its schema alone for no rows)."""
    tables = [pyarrow.ipc.open_stream(c).read_all() for c in chunks]
    if not tables:
        raise pa.ArrowInvalid('the warehouse answered no chunk, not even its columns')
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def _on_planner_threads() -> Callable[..., Any]:
    """Where a planner's calls run: the compiler's threads (the native library's)."""
    return lambda f, *a: _workers().submit(f, *a).result()


def _arrow_rows(rows: pa.Table, planned: str, receipt: dict[str, Any] | None = None) -> Answer:
    """A plan-and-run's rows, as Arrow, the planner's answer in their schema's metadata (``legend.plan``), and where
    they were read when not here (``legend.receipt``: a warehouse table's, its statement and its reader)."""
    kept = {**(rows.schema.metadata or {}), _PLAN_KEY: planned.encode('utf-8')}
    if receipt is not None:
        kept[b'legend.receipt'] = json.dumps(receipt).encode('utf-8')
    rows = rows.replace_schema_metadata(kept)
    sink = pa.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, rows.schema) as writer:
        writer.write_table(rows)
    return Answer(200, 'application/vnd.apache.arrow.stream', sink.getvalue().to_pybytes())


def _planned(said: dict[str, str], status: int = 200) -> Answer:
    """A plan-and-run that answered no rows: the planner's own answer, and why its SQL was refused when it was."""
    return Answer(status, 'application/json', json.dumps(said).encode('utf-8'))


def _answered(work: Callable[[str], Answer], body: str) -> Answer:
    """A page's planner call answered, never a dropped call: what the call itself does not answer is a bug, logged whole
    and said (500), as the API's are (``Engine._api``)."""
    try:
        return work(body)
    except Exception as failure:
        traceback.print_exc()
        return _said(500, f'{type(failure).__name__}: {failure}')


def duckdb_extensions() -> Path | None:
    """DuckDB's extensions as legend-lite carries them, laid out as DuckDB's own extension folder
    (``v1.5.5/<platform>/httpfs.duckdb_extension``): LEGEND_LITE_DUCKDB_EXTENSIONS, else the folder inside this package;
    None when it holds no httpfs for this DuckDB (a build without it: no remote file is read)."""
    folder = _extensions_folder()
    return folder if (folder / f'v{duckdb.__version__}' / _duckdb_platform() / 'httpfs.duckdb_extension').is_file() else None


def _extensions_folder() -> Path:
    import os

    explicit = os.environ.get('LEGEND_LITE_DUCKDB_EXTENSIONS')
    return Path(explicit) if explicit else Path(__file__).with_name('_duckdb')


def remote_url(url: str) -> bool:
    """Whether a page's URL is a remote one this engine reads: exactly ``<scheme>://<host>...`` for one of the schemes,
    as written -- DuckDB matches its remote file systems by that very prefix, case and all, and reads anything else
    from this machine, so nothing is normalised first (no case folded; no space or control character anywhere)."""
    if any(ord(c) <= 0x20 or ord(c) == 0x7F for c in url):
        return False
    scheme = next((s for s in _REMOTE_SCHEMES if url.startswith(f'{s}://')), None)
    return scheme is not None and urlsplit(url).netloc != ''


@functools.cache
def _duckdb_platform() -> str:
    """The platform DuckDB names its extensions by here (``osx_arm64``, ``linux_amd64``, ``windows_amd64``, ...)."""
    return _host.ask(duckdb.connect(), {'op': 'platform'})[0][0]


def private(path: str) -> bool:
    """Whether a call reads the engine's data -- the API, a frame's cube, its version -- and so, over HTTP, carries the
    token. The site's files are the page's own and carry no secret."""
    return path.startswith('/api/') or _model_home(path) or path in (
        '/cube.json', '/version.json', '/page.json', _FILE_IN, _FILE_PART, _FILE_OUT, _REMOTE, _SETTINGS, _SERVICES,
        _PLAN_AND_RUN, _PLAN, _WAREHOUSE)


def _saved_query(path: str) -> bool:
    """A call of the query store's (``/api/pure/v1/query``, ``.../{id}``, ``.../search``, ...), DataCube's saved pages'
    (``.../dataCube...``) among them."""
    return path == _QUERIES or path.startswith(_QUERIES + '/')


def _stored(entry: str, folder: Path, *asked: str) -> Answer:
    """One call of a store the native library answers (the saved queries, the saved pages), as this user: its answer as
    the store gave it, a failure of the library's said (500)."""
    try:
        status, answered = store_call(entry, folder, *asked, _QUERY_USER)
    except Exception as failure:
        traceback.print_exc()
        return _said(500, f'{type(failure).__name__}: {failure}')
    if status == 204:
        return Answer(204, 'text/plain; charset=utf-8', b'')
    return Answer(status, 'application/json', answered.encode('utf-8'))


def _saved_cube(path: str) -> bool:
    """A call of DataCube's saved pages' (``/api/pure/v1/query/dataCube``, ``.../{id}``, ``.../search``)."""
    return path == _CUBES or path.startswith(_CUBES + '/')


def _app_of(path: str) -> str | None:
    """The app whose site a path is in (``/query/...``, ``/studio/...``), or None."""
    head = path.split('/', 2)[1] if path.startswith('/') else ''
    return head if head in _APPS and path.startswith(f'/{head}/') else None


def _app_site(app: str) -> Path | None:
    """An app's built site: LEGEND_LITE_<APP>_SITE, else the one shipped inside this package (``_apps/<app>``)."""
    explicit = os.environ.get(f'LEGEND_LITE_{app.upper()}_SITE')
    site = Path(explicit) if explicit else Path(__file__).with_name('_apps') / app
    return site if site.is_dir() else None


def _model_home(path: str) -> bool:
    """A call of the model home's: the SDLC's API or the Depot's."""
    return path.startswith(_SDLC_API) or path.startswith(_DEPOT_API)


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


class NativePlanner:
    """WHERE THE ENGINE PLANS (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §4.1, R3): legend-lite's compiler in this
    process, the native library -- the one plane a Python host has (a host plans in process: the program's §1.1, the
    user's ruling of 2026-10-15; a remote engine is a client feature over the remote side's own models and data, never a
    mode of the host). Every call the PAGE makes that the compiler answers -- the engine's pure/v1 API: a query's types,
    a parse, a print, and an execute's plan, whose SQL Python then runs on its own DuckDB (R4) -- is answered here. Its
    ``plane`` is the word the page's status bar says, and its ``label`` the plane's, as every host says its plane."""

    plane = 'local'
    label = 'Plan local (in Python: the native library)'

    def answer(self, path: str, query: str, body: str) -> compiler.Answer:
        return compiler.pure_v1(path, query, body)

    def execute_plan(self, body: str, models: list[str]) -> compiler.Answer:
        return compiler.execute_plan(body, models)

    def execute_json_plan(self, body: str, models: list[str]) -> compiler.Answer:
        return compiler.execute_json_plan(body, models)

    def execute_json_answer(self, plan: str, wire: str) -> compiler.Answer:
        return compiler.execute_json_answer(plan, wire)

    def refusal(self, message: str) -> compiler.Answer:
        return compiler.refusal(message)

    def plan(self, query: str, held: compiler.HeldModel, runtime: str) -> str:
        # the planner function the tab's worker calls (planJson), over the model this process holds
        return library().call('lite_plan_json_held', held.handle, query, runtime)

    def read(self, answer: str) -> compiler.Plan:
        return compiler.read_planned(answer)

    def refused_plan(self, message: str) -> str:
        return f'ERR\nMODEL\n{message}'


class _WarehouseTable:
    """A warehouse table a page opened on Python (C3): the model Python wrote for it and holds, read as one person --
    its live plans made against its runtime and run on the warehouse, through the binding -- and its copy (Snap), in
    the frames' DuckDB under the table's own name, which the model's snap runtime reads. The copy is one per table, as
    the tab's is one per tab: the grids that snapped it hold it (``holders``), and it is dropped when the last lets
    go."""

    def __init__(self, held: compiler.HeldModel, written: dict[str, Any], schema: str, name: str,
                 where: warehouse.Warehouse, catalog: str, database_type: str = 'DuckDB') -> None:
        self.held = held
        # its catalog's database type (the catalog entry's): what a statement run on the warehouse is written for
        self.database_type = database_type
        self.written = written
        self.schema = schema
        self.name = name
        self.warehouse = where
        self.catalog = catalog
        self.holders = 0
        # what the copy holds (its count and the pull's receipt), answered to a grid that snaps it while it is there
        self.frozen: dict[str, Any] | None = None
        # the schema the copy made in the frames' DuckDB, dropped with the copy when it was the copy's alone
        self.made_schema = False
        # the copy's changes, one at a time: a release while a snap copies waits for it, never lost
        self.copying = threading.Lock()

    @property
    def runtime(self) -> str:
        return self.written['runtime']

    @property
    def snap_runtime(self) -> str:
        return self.written['snapRuntime']

    @property
    def snapped(self) -> bool:
        return self.holders > 0

    def refused_runtime(self, runtime: str) -> str | None:
        """Why a query of this table may not name ``runtime``, or None: its own runtime (live, on the warehouse), or its
        snap runtime while a copy is held. Asked by its plan-and-run and its dry run alike (``plan.json``)."""
        if runtime == self.runtime or (runtime == self.snap_runtime and self.snapped):
            return None
        return (f'{self.schema}.{self.name} is read on its warehouse, through its own runtime, or snapped, through its '
                "copy's")

    def reads(self, base_url: str, catalog: str) -> bool:
        return self.warehouse.base_url.rstrip('/') == base_url.rstrip('/') and self.catalog == catalog


class Engine:
    """legend-engine's ``pure/v1`` API over the frames' tables, with no transport: ``answer(method, path, query,
    body)`` is one call, answered as HTTP would carry it. ``site``: a directory whose files are answered too
    (DataCube's built pages). ``WebServer`` serves it to a browser tab; a notebook's cube sends it its widget's
    messages."""

    def __init__(self, frames: Frames, site: str | Path | None = None,
                 headless: Callable[[], Any] | None = None, planner: NativePlanner | None = None,
                 pages: str | Path | None = None) -> None:
        """``headless``: DataCube's own non-screen code over the frames' database (legend_lite._headless), made when
        first asked for -- what reads a page's file into it (file.json). Absent, the engine takes no file, and says so
        (services.json). ``planner``: the native library's planner (a test's own, to see each call it answers).
        ``pages``: the folder the page's Save and Open… keep pages in (step 2d, B6): this user's (``home()/pages``) when
        not given."""
        self.frames = frames
        self._headless = headless
        # where the frames' DuckDB was pointed for its extensions (a remote file's httpfs), once it was
        self._extensions: Path | None = None
        # the page files coming in a part at a time (file-part.json), each by the name Python gave it: where its bytes
        # are written so far; one part is written at a time
        self._uploads: dict[str, Path] = {}
        self._uploads_lock = threading.Lock()
        # the warehouses signed in to (by address), and the tables opened from them, by the model Python wrote for each
        # (C3): a plan over that model runs on the warehouse, as the signed-in user
        self._warehouse_sessions: dict[str, warehouse.Session] = {}
        self._warehouse_tables: dict[str, _WarehouseTable] = {}
        self._warehouse_lock = threading.Lock()
        # the binding's calls on the compiler's threads, never a connection's (warehouse.set_library_thread)
        warehouse.set_library_thread(_on_compiler_threads)
        self.planner = planner if planner is not None else NativePlanner()
        # a project's own rows, when Query or Studio served here runs one of its queries (C5): each project
        # version in a database of its own inside the frames' DuckDB, made by the native library's statements
        self.projects = ProjectDatabases(frames.connection, guard=frames.holding)
        self._pages_folder = Path(pages) if pages is not None else None
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
        # each page's last version, kept when it closes: a page shown again goes on from it, so what a tab still open
        # on its earlier showing says is of a version replaced
        self._page_versions: dict[str, int] = {}
        # each page's document as the open page says it is now (its changes made in DataCube included: page.read()).
        # Documents are kept as the compiler's exact JSON reads them (_json): a calculated column's numbers exact
        self._read: dict[str, dict[str, Any]] = {}

    def answer(self, method: str, path: str, query: str, body: str | None) -> Answer:
        """One call: ``POST`` to the API, or ``GET`` of a frame's cube (``cube.json``), its version (``version.json``) or
        a file of the site. It waits for its answer, which the compiler's threads compute when it needs the compiler,
        so it is called from a thread of the caller's own (a connection's, a message's), never a compiler thread."""
        if _saved_cube(path):
            # on the caller's thread, as the saved queries': it waits for the folder's lock holding no compiler thread
            return self._saved_cubes(method, path, query, body or '')
        if _saved_query(path):
            # on the caller's thread, as the model home's: it waits for the folder's lock holding no compiler thread
            return self._saved_queries(method, path, query, body or '')
        if _model_home(path):
            # on the caller's thread: it waits for the repository's lock (another process's call) holding no compiler
            # thread, and only the library's call goes to one
            return self._model_home(method, path, query, body or '')
        if path == _CURRENT_USER and method == 'GET':
            # who a page's caller is: this user (the stores name the same; there is no sign-in)
            return Answer(200, 'application/json', json.dumps(_QUERY_USER).encode('utf-8'))
        if method == 'POST':
            if path.startswith(_LITE_API):
                # legend-lite's own routes (its whole-model compile), answered as its server answers them
                return _workers().submit(self._api, path, query, body or '').result()
            if path == '/page.json':
                return self._page_said(query, body or '')
            if path == _FILE_PART:
                return self._file_part(body or '')
            if path == _FILE_IN:
                return self._file_in(body or '')
            if path == _FILE_OUT:
                return self._file_out(body or '')
            if path == _REMOTE:
                return self._remote(body or '')
            if path == _SETTINGS:
                return self._settings_kept(body or '')
            if path == _PLAN_AND_RUN:
                table = self._warehouse_table_of(body or '')
                if table is not None:
                    # a warehouse table's plan runs there: its HTTP on the caller's thread, holding no compiler thread
                    return _answered(lambda b: self._warehouse_plan_and_run(table, b), body or '')
                return _workers().submit(_answered, self._plan_and_run, body or '').result()
            if path == _PLAN:
                return _workers().submit(_answered, self._plan, body or '').result()
            if path == _WAREHOUSE:
                # on the caller's thread: the warehouse's HTTP holds no compiler thread, and only the library's calls go
                # to one; never a dropped call
                return _answered(self._warehouse, body or '')
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
        if path == _SERVICES:
            return Answer(200, 'application/json', json.dumps(self.services()).encode('utf-8'))
        if path == _SETTINGS:
            try:
                kept = Settings().read()
            except OSError as failure:
                return _said(500, f'the settings could not be read: {type(failure).__name__}: {failure}')
            return Answer(200, 'application/json', json.dumps(kept).encode('utf-8'))
        return self._file(path)

    def services(self) -> dict[str, Any]:
        """What this engine offers the page beside its frames (services.json, asked once as the page starts): whether
        it reads a page's file, and where it plans -- the plane it plans on (Python's own, in this process: R3's "shown",
        as every host says its plane), and every plane it offers: that one, the only one a Python host has."""
        plane = {'word': self.planner.plane, 'label': self.planner.label}
        # where the page's saved pages are, and whose: the folder's one owner, this user (as the browser's store is
        # 'this browser''s)
        pages = {'where': str(self.pages_folder()), 'user': _QUERY_USER}
        # where the model home keeps the projects (B3): Create a Class… and the rest are offered over it
        model_home = {'where': str(self.model_home_folder())}
        # where the query store keeps the saved queries (B5), and whose they are
        queries = {'where': str(queries_folder()), 'user': _QUERY_USER}
        # a warehouse's tables (C3): Python signs in to one as this user and reads its tables Live; and the apps it
        # serves (E), whose links a page may give (Studio's, over the model home's projects)
        apps = [app for app in _APPS if _app_site(app) is not None]
        return {'files': self.takes_files(), 'remote': self.takes_remote(), 'pages': pages, 'modelHome': model_home,
                'queries': queries, 'warehouse': True, 'apps': apps, 'plane': plane, 'planes': [plane]}

    def pages_folder(self) -> Path:
        """Where the page's saved pages are kept: the folder given, else this user's (``home()/pages``), as it is now."""
        return self._pages_folder if self._pages_folder is not None else pages_folder()

    def model_home_folder(self) -> Path:
        """Where the model home's projects are: a git repository in this user's folder, as it is now."""
        return model_home_folder()

    def _model_home(self, method: str, path: str, query: str, body: str) -> Answer:
        """One call of the model home (B3): answered by the native library's SDLC or Depot -- the rules the page's
        in-page SDLC and the model home's server run -- over this user's repository, under its lock (several Python
        processes share it: two notebooks at once). Never a dropped call: a failure of the library's is said (500)."""
        if method not in ('GET', 'POST', 'DELETE'):
            return _said(405, "the model home answers GET, POST and DELETE")
        sdlc = path.startswith(_SDLC_API)
        root = _SDLC_API if sdlc else _DEPOT_API
        target = '/' + path[len(root):] + (f'?{query}' if query else '')
        from .model import home_call
        try:
            status, answered = home_call(self.model_home_folder(), 'sdlc' if sdlc else 'depot', method, target, body)
        except OSError as refused:
            # the library's own refusal, in its words
            return _said(500, str(refused))
        except Exception as failure:
            traceback.print_exc()
            return _said(500, f'{type(failure).__name__}: {failure}')
        if not answered:
            return Answer(int(status), 'text/plain; charset=utf-8', b'')
        return Answer(int(status), 'application/json', answered.encode('utf-8'))

    def _saved_queries(self, method: str, path: str, query: str, body: str) -> Answer:
        """One call of the query store (B5): answered by legend-lite's saved-query rules in the native library -- the
        ones its server runs and the page runs compiled to WebAssembly -- over this user's folder (``ll.home()/queries``,
        one JSON file per query, the server's layout), under its lock (several Python processes share it). Its caller
        is this user: there is no sign-in."""
        if method not in ('GET', 'POST', 'PUT', 'DELETE'):
            return _said(405, 'the query store answers GET, POST, PUT and DELETE')
        return _stored('lite_saved_queries', queries_folder(), method, path[len(_QUERIES):], query, body)

    def _saved_cubes(self, method: str, path: str, query: str, body: str) -> Answer:
        """One call of DataCube's saved pages (B6): answered by legend-lite's saved-page rules in the native library --
        the ones its server runs and the page runs compiled to WebAssembly -- over this user's folder (``home()/pages``,
        one file each, as the page's records were kept), under its lock. Its caller is this user: there is no sign-in."""
        if method not in ('GET', 'POST', 'PUT', 'DELETE'):
            return _said(405, "DataCube's saved pages answer GET, POST, PUT and DELETE")
        return _stored('lite_saved_cubes', self.pages_folder(), method, path[len(_CUBES):], query, body)

    def _settings_kept(self, body: str) -> Answer:
        """The page's settings kept whole (one JSON object, the page's own: never read here), in this user's folder, so
        every notebook and run shows them (B2): the browser's storage is the page's address's, and Python's address is
        new each run."""
        try:
            values = json.loads(body)
        except ValueError:
            return _said(400, "DataCube's settings are one JSON object")
        if not isinstance(values, dict):
            return _said(400, "DataCube's settings are one JSON object")
        try:
            Settings().write(values)
        except OSError as failure:
            return _said(500, f'the settings could not be kept: {type(failure).__name__}: {failure}')
        return Answer(204, 'text/plain; charset=utf-8', b'')

    def takes_files(self) -> bool:
        """Whether a page's file can be read into the frames' database here: this engine was given DataCube's own code,
        headless, so the page offers its files and examples. Its V8 is the datacube extra's, there whenever this module
        is (_extra.py), and its bundle is the wheel's: one missing is said where a file is read, never hidden."""
        return self._headless is not None

    def takes_remote(self) -> bool:
        """Whether a page's remote file can be read here (``remote_refusal`` says why not)."""
        return self.remote_refusal() is None

    def remote_refusal(self) -> str | None:
        """Why a page's remote file cannot be read here, or None: it needs DataCube's own code (headless), DuckDB's
        httpfs extension for this very DuckDB where the package carries it (nothing is downloaded), and a database
        legend-lite made -- its extension settings are changed for good, which a database of the person's own is not."""
        if not self.takes_files():
            return 'legend-lite reads one with DataCube\'s own code, headless, which this engine was not given'
        if not self.frames.owns_connection:
            return 'it is read on a DuckDB legend-lite made, and these frames are on a database of your own'
        if duckdb_extensions() is None:
            folder = _extensions_folder()
            carried = sorted(p.name[1:] for p in folder.glob('v*')) if folder.is_dir() else []
            return (f'legend-lite carries DuckDB\'s httpfs for duckdb {", ".join(carried)}, and this Python has duckdb '
                    f'{duckdb.__version__}' if carried else 'this legend-lite carries no httpfs extension for DuckDB')
        return None

    def _remote(self, body: str) -> Answer:
        """A page's remote file read through a view of its own in the frames' database (``{"url", "name", "s3"?}``;
        docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md, step 2b): its URL one of the remote schemes, never this machine's
        files, checked before anything runs; read by DataCube's own code (upload.ts's mountRemoteView, the tab's, run
        headless) with the httpfs extension the package carries. The view's model is served beside the frames' from
        then on, and the answer is its model, as the tab's mount says it. The page sends no SQL."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a remote file is {"url", "name", "s3"?}, as JSON')
        if not (isinstance(asked, dict) and isinstance(asked.get('url'), str) and isinstance(asked.get('name'), str)
                and (asked.get('s3') is None or isinstance(asked['s3'], dict))):
            return _said(400, 'a remote file is {"url", "name", "s3"?}: a URL, a name, and its keys when it needs them')
        url = asked['url']
        if not remote_url(url):
            return _said(400, f'a remote file is read from {", ".join(s + "://" for s in _REMOTE_SCHEMES)}, never from '
                              f'this machine: {url!r} is not such a URL (a file of yours: choose it under Files)')
        refused = self.remote_refusal()
        if refused is not None:
            return _said(404, f'this engine reads no remote file: {refused}')
        extensions = duckdb_extensions()
        assert extensions is not None
        assert self._headless is not None
        try:
            # asked for before the frames are held (the session's lock first, then the frames')
            headless = self._headless()
            with self.frames.reading_file(asked['name'], view=True) as name:
                self._extensions_here(extensions)
                mount = {'url': url, 'name': name, **({'s3': asked['s3']} if asked.get('s3') else {})}
                # the source's extensions and keys first, then what it names checked, before any view of it reads it
                scan = json.loads(headless.call('remoteAccess', json.dumps(mount)))
                if scan['format'] == 'iceberg':
                    refused = self._iceberg_refusal(scan['url'])
                    if refused is not None:
                        raise ValueError(refused)
                said = headless.call('mount', json.dumps(mount))
                self.frames.keep_file(name, json.loads(said)['model'], view=True)
        except ValueError as refused:
            return _said(400, str(refused))
        except Exception as failure:
            # the file not read (not there, its keys refused, a format DuckDB cannot read): said, as the tab says it
            return _said(422, f'{type(failure).__name__}: {failure}')
        return Answer(200, 'application/json', said.encode('utf-8'))

    def _iceberg_refusal(self, url: str) -> str | None:
        """Why a page's Iceberg table is not read here, or None: its metadata names its files by path, so every manifest
        list its snapshots name, every manifest those name, and every data and delete file its current snapshot reads
        must be a remote URL (``remote_url``), or a remote table could make this DuckDB read this machine's files (R4).
        Each level is read only once the level that names it is checked, and only through the functions that report a
        file they cannot read: DuckDB 1.5.5's ``iceberg_scan`` ends the process instead, so no view of a table is made
        before every manifest it reads has been read here. Manifest lists of format versions 1 and 2 are read together,
        by name (measured 2026-10-13). A table refused keeps the keys its page gave on the connection, scoped to its own
        URL, as a mount that fails does (PARK-29)."""
        connection = self.frames.connection
        lists = [row[0] for row in _host.ask(connection, {'op': 'icebergManifestLists', 'url': url})]
        named = lists
        if all(remote_url(path) for path in named) and lists:
            named = [row[0] for row in _host.ask(connection, {'op': 'icebergManifests', 'lists': lists})]
            if all(remote_url(path) for path in named):
                named = [row[0] for row in _host.ask(connection, {'op': 'icebergDataFiles', 'url': url})]
        local = next((path for path in named if not remote_url(path)), None)
        if local is None:
            return None
        return (f'an Iceberg table is read only when every file it names is a remote one, never this machine\'s, and '
                f'this one names {local!r}')

    def _extensions_here(self, folder: Path) -> None:
        """The frames' DuckDB pointed at the extensions the package carries, once (the frames held): DataCube's INSTALL
        finds httpfs installed there and LOAD loads it, and an extension not there is never downloaded -- the
        repository it would be fetched from is the same folder, and installing on its own is off."""
        if self._extensions is not None:
            return
        _host.run(self.frames.connection, {'op': 'extensionDirectory', 'folder': str(folder)})
        self._extensions = folder

    def _file_part(self, body: str) -> Answer:
        """A part of a page's file, in order (``{"upload"?, "at", "bytes"}``, its bytes base64): the bytes the tab's DuckDB
        would be given (upload.ts ``fileParts``), sent as the page reads them, so no file is too big to send. The first
        part, with no ``upload``, begins one, named by Python (``{"upload"}`` answers it); each next names it and starts
        where the bytes so far end (``at``), so a part sent twice or out of order is refused, never written twice. The
        bytes are written where Python's DuckDB reads them (the headless engine's folder, under a name of Python's, gone
        with that folder if no file.json ever reads them); file.json reads them as one file."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a part of a file is {"upload"?, "at", "bytes"}, as JSON')
        upload, at = (asked.get('upload'), asked.get('at')) if isinstance(asked, dict) else (None, None)
        if not (isinstance(asked, dict) and isinstance(asked.get('bytes'), str) and (upload is None or isinstance(upload, str))
                and isinstance(at, int) and not isinstance(at, bool) and at >= 0):
            return _said(400, 'a part of a file is {"upload"?, "at", "bytes"}: its upload a string, at a count of bytes, '
                              'its bytes base64')
        if not self.takes_files():
            return _said(404, 'this engine reads no file: legend-lite reads one with DataCube\'s own code, headless, '
                              'which this engine was not given')
        assert self._headless is not None
        try:
            data = base64.b64decode(asked['bytes'], validate=True)
        except binascii.Error:
            return _said(400, "a file's bytes are base64")
        try:
            folder = self._headless().folder
        except OSError as failure:
            return _said(500, str(failure))
        with self._uploads_lock:
            if upload is None:
                if at != 0:
                    return _said(400, 'an upload begins at its first byte (at 0)')
                upload = secrets.token_hex(16)
                path = folder / f'page_upload_{upload}'
                path.write_bytes(b'')
                self._uploads[upload] = path
            else:
                path = self._uploads.get(upload)
                if path is None:
                    return _said(404, f'no file is coming in as {upload!r}: begin it with its first part')
            size = path.stat().st_size
            if at != size:
                return _said(409, f'the part starts at byte {at}, and the file so far ends at byte {size}')
            with path.open('ab') as file:
                file.write(data)
        return Answer(200, 'application/json', json.dumps({'upload': upload, 'size': size + len(data)}).encode('utf-8'))

    def _file_in(self, body: str) -> Answer:
        """A page's file read into a table of its own in the frames' database (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md
        §4.3): ``{"upload", "name", "table"}``, its bytes the parts that upload brought (file-part.json), written where
        Python's DuckDB reads them. DataCube's own code reads that file into the table (upload.ts's readFileInto, run
        headless): only the path crosses into the engine, and Python and the tab read a file the same way. The table is
        a copy, so the file goes at once, read or not. Its model is served beside the frames' from then on (the only
        models a query may be over), and the answer is what arrived, as upload.ts says it. The page sends no SQL."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a file is {"upload", "name", "table"}, as JSON')
        if not (isinstance(asked, dict) and all(isinstance(asked.get(k), str) for k in ('upload', 'name', 'table'))):
            return _said(400, 'a file is {"upload", "name", "table"}, each a string')
        if not self.takes_files():
            return _said(404, 'this engine reads no file: legend-lite reads one with DataCube\'s own code, headless, '
                              'which this engine was not given')
        assert self._headless is not None
        with self._uploads_lock:
            path = self._uploads.pop(asked['upload'], None)
        if path is None:
            return _said(404, f'no file came in as {asked["upload"]!r}: send its parts first (file-part.json)')
        try:
            # asked for before the frames are held: the session takes its own lock first, then the frames' (register)
            headless = self._headless()
            # the table the page asked for when it is free, else the next free name: the answer says which
            with self.frames.reading_file(asked['table']) as table:
                said = headless.call('ingest', json.dumps({'path': str(path), 'name': asked['name'], 'table': table}))
                read = json.loads(said)
                self.frames.keep_file(table, read['model'])
        except ValueError as refused:
            return _said(400, str(refused))
        except Exception as failure:
            # the file not read (DuckDB could not, a column no model holds): said, as the tab says it
            return _said(422, f'{type(failure).__name__}: {failure}')
        finally:
            # the table is a copy: the file is not needed. One Windows still holds open is left for the headless
            # engine's folder, which goes with it -- never a read that worked turned into a refusal
            with contextlib.suppress(OSError):
                path.unlink(missing_ok=True)
        return Answer(200, 'application/json', said.encode('utf-8'))

    def _file_out(self, body: str) -> Answer:
        """A page's file let go of (``{"name", "table"}``): its table dropped by DataCube's own code, its model served no
        more. A table that is no file's is refused: a page drops nothing else."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a file let go of is {"name", "table"}, as JSON')
        if not (isinstance(asked, dict) and all(isinstance(asked.get(k), str) for k in ('name', 'table'))):
            return _said(400, 'a file let go of is {"name", "table"}, each a string')
        table = self.frames.file_table(asked['table'])
        if table is None or not self.takes_files():
            return _said(404, f'{asked["table"]!r} is no table a page\'s file was read into')
        assert self._headless is not None
        try:
            # asked for before the frames are held (the session's lock first, then the frames')
            headless = self._headless()
            with self.frames.holding():
                # dropped first, by the name Frames gave it, and only then served no more: a drop that fails leaves the
                # table served, and the page may let go of it again
                headless.call('forget', json.dumps({'table': table}))
                self.frames.forget_file(table)
        except Exception as failure:
            return _said(422, f'{type(failure).__name__}: {failure}')
        return Answer(204, 'text/plain; charset=utf-8', b'')

    def _api(self, path: str, query: str, body: str) -> Answer:
        """An API call's answer, computed on the compiler's pool."""
        try:
            if path == _EXECUTE:
                return self._execute(body) if _arrow_requested(query) else self._execute_json(body)
            answer = self.planner.answer(path, query, body)
        except Exception as failure:
            # never a dropped call: the database refusing is an answer; anything else is a bug, logged whole, and
            # answered the same way (as legend-lite's server answers both)
            if not isinstance(failure, duckdb.Error):
                traceback.print_exc()
            message = f'{type(failure).__name__}: {failure}'
            try:
                answer = self.planner.refusal(message)
            except Exception:
                # the compiler itself failing: still an answer, its own words
                traceback.print_exc()
                return _said(500, message)
        return Answer(answer.status, answer.content_type, answer.body.encode('utf-8'))

    def _warehouse(self, body: str) -> Answer:
        """A warehouse on a page (C3). ``{"op": "connect", "url", "user", "password"}``: signed in as the person running
        Python, the session answered -- its address, who the warehouse says it is, when it ends; never its token -- with
        what they may read. ``{"op": "table", "url", "catalog", "schema", "name"}``: a table listed there, its model
        written by the tab's own writer (``table_model``: read-only, its copy planned for DuckDB) and held, answered as
        that writer answers it. ``snap`` and ``release``: its copy (``_warehouse_snap``). A refusal is its words; an
        expired session 401, a warehouse's refusal 422."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a warehouse call is JSON')
        if not isinstance(asked, dict) or not isinstance(asked.get('op'), str):
            return _said(400, 'a warehouse call is {"op", ...}')
        try:
            if asked['op'] in ('snap', 'release'):
                return self._warehouse_snap(asked)
            if not isinstance(asked.get('url'), str):
                return _said(400, 'a warehouse call names its warehouse ("url")')
            url = asked['url'].rstrip('/')
            if asked['op'] == 'connect':
                if not (isinstance(asked.get('user'), str) and isinstance(asked.get('password'), str)):
                    return _said(400, 'a sign-in is {"user", "password"}, each a string')
                session, objects = warehouse.connect(url, asked['user'], asked['password'])
                with self._warehouse_lock:
                    # the sign-in kept as a reader of its own (its token refreshed as it nears its end), for the next
                    # table opened from this warehouse
                    self._warehouse_sessions[url] = warehouse.Warehouse(session)
                    # the tables open from it go on with the new token when it is the same person (P2-297)
                    for table in self._warehouse_tables.values():
                        if table.warehouse.base_url.rstrip('/') == url and table.warehouse.principal == session.principal:
                            table.warehouse.renew(session)
                said = {'session': {'baseUrl': session.base_url, 'principal': session.principal,
                                    'expiresAt': session.expires_at, 'token': ''}, 'objects': objects}
                return Answer(200, 'application/json', json.dumps(said).encode('utf-8'))
            if asked['op'] == 'table':
                return self._warehouse_table(url, asked)
        except warehouse.SessionExpired as expired:
            return _said(401, str(expired))
        except warehouse.WarehouseError as refused:
            return _said(422, str(refused))
        return _said(400, f"no such warehouse call: {asked['op']}")

    def _warehouse_table(self, url: str, asked: dict[str, Any]) -> Answer:
        """A listed table opened: its columns read from the warehouse now (what the person may read, as the catalog has
        it), its model written and held. A model's text names a plan's table by itself, so one table of that text is
        open at a time: the same table and columns from another warehouse or catalog, or as another person, is refused
        while the first is open. Nothing waits on the compiler's threads while this engine's warehouse lock is held:
        the compiler's threads take that lock (``_plan``)."""
        if not all(isinstance(asked.get(k), str) for k in ('catalog', 'schema', 'name')):
            return _said(400, 'a table is {"catalog", "schema", "name"}, each a string')
        catalog, schema, name = asked['catalog'], asked['schema'], asked['name']
        with self._warehouse_lock:
            signed = self._warehouse_sessions.get(url)
        if signed is None:
            return _said(401, f'sign in to the warehouse at {url} first')
        found = next((o for o in signed.objects() if (o['catalog'], o['schema'], o['name']) == (catalog, schema, name)),
                     None)
        if found is None:
            return _said(404, f'{schema}.{name} is not a table {signed.principal} may read in {catalog}')
        spec = {'table': name, 'schema': schema, 'convertible': False, 'databaseType': found['databaseType'],
                'snapDatabaseType': 'DuckDB', 'columns': [{**c, 'dataType': c['type']} for c in found['columns']]}
        written = _workers().submit(compiler.table_model, spec).result()
        if self.frames.held_for(written['model']) is not None:
            # a frame or a page's file whose model is this text: the warehouse's would answer for it
            return _said(409, f'a frame or file of this Python is {schema}.{name} with the same columns: rename one')
        held = _workers().submit(compiler.HeldModel, written['model']).result()
        refusal, spare = None, held
        with self._warehouse_lock:
            kept = self._warehouse_tables.get(written['model'])
            if kept is None:
                self._warehouse_tables[written['model']] = _WarehouseTable(
                    held, written, schema, name, warehouse.Warehouse(signed.session, catalog), catalog,
                    found['databaseType'])
                spare = None
            elif not kept.reads(url, catalog):
                refusal = (f'{schema}.{name} is open already, with the same columns, from {kept.warehouse.base_url} '
                           f'({kept.catalog}): one of the two at a time')
            elif kept.warehouse.principal != signed.principal:
                refusal = (f'{schema}.{name} is open already as {kept.warehouse.principal}: what a grid reads is one '
                           f"person's, so it is read as them until Python restarts")
        if spare is not None:
            _workers().submit(spare.close).result()
        if refusal is not None:
            return _said(409, refusal)
        said = {k: v for k, v in written.items() if k != 'accessor'}
        return Answer(200, 'application/json', json.dumps(said).encode('utf-8'))

    def _warehouse_snap(self, asked: dict[str, Any]) -> Answer:
        """SNAP, ON PYTHON (C3; engine-client/src/snap.ts, the tab's, is its twin): ``{"op": "snap", "model", "pull"}`` --
        the pull, Pure (its source's every column), planned with the table's live runtime and counted where the rows
        are against the same limit, its Arrow chunks copied into the frames' DuckDB under the table's own name (the
        model's conversions applied, as the tab's copy applies them), read from then on through the snap runtime; the
        grid that snapped holds the copy, and a grid that snaps it while it is there holds the same copy, as it is (one
        copy per table, as the tab's is one per tab: a second pull would change the first grid's rows under it).
        ``{"op": "release", "model"}``: that grid lets it go, and the copy is dropped when no grid holds it. Never a
        frame's or a file's table, nor one this engine did not make; one change of the copy at a time."""
        model = asked.get('model')
        with self._warehouse_lock:
            table = self._warehouse_tables.get(model) if isinstance(model, str) else None
        if table is None:
            return _said(404, 'this engine has no warehouse table of that model open')
        with table.copying:
            with self.frames.holding():
                # the shared connection, under its lock as every other use of it
                database = _host.ask(self.frames.connection, {'op': 'currentDatabase'})[0][0]
            # the copy by its whole name: a schema the warehouse names (temp, an attached database's) never reaches
            # another catalog's table
            target = _host.name(table.name, schema=table.schema, database=database)
            if asked['op'] == 'release':
                with self.frames.holding():
                    if table.holders == 1:
                        self._drop_copy(table, database, target)
                    table.holders = max(0, table.holders - 1)
                return Answer(200, 'application/json', b'{}')
            if not isinstance(asked.get('pull'), str):
                return _said(400, 'a snap is {"model", "pull"}, the pull its lambda as JSON')
            if table.snapped and table.frozen is not None:
                # held by another grid already: this one holds the same copy, as it is
                table.holders += 1
                return Answer(200, 'application/json', json.dumps(table.frozen).encode('utf-8'))
            on = _on_planner_threads()
            planned = on(self.planner.plan, asked['pull'], table.held, table.runtime)
            try:
                plan = on(self.planner.read, planned)
            except LegendError as refused:
                return _said(422, f'the snap could not be planned: {refused}')
            try:
                # counted where the rows are, before anything is copied (the tab's preflight), by the catalog's own
                # database's statement (a Postgres catalog's, Postgres's: D5)
                (counting,) = _host.statements({'op': 'count', 'from': {'query': plan.sql}},
                                               database_type=table.database_type)
                counted = _arrow_of(table.warehouse.arrow_chunks(counting))
                rows = int(counted.column(0)[0].as_py()) if counted.num_rows else 0
                if rows > _MAX_SNAP_ROWS:
                    return _said(422, f'{rows:,} rows exceeds the {_MAX_SNAP_ROWS:,} row snap limit. Narrow the filter, '
                                      'or stay live.')
                receipt: dict[str, Any] = {}
                pulled = _arrow_of(table.warehouse.arrow_chunks(plan.sql, receipt.update))
            except warehouse.SessionExpired as expired:
                return _said(401, str(expired))
            except (warehouse.WarehouseError, pa.ArrowInvalid) as refused:
                return _said(422, f'the snap could not be pulled: {refused}')
            conversions = table.written.get('conversions') or []
            landing = '__legend_lite_snap'
            with self.frames.holding():
                if not table.snapped and (table.schema.lower() == 'main' and self.frames.taken(table.name)
                                          or self._table_exists(database, table.schema, table.name)):
                    return _said(409, f'{table.schema}.{table.name} is a table of this Python\'s already: its copy '
                                      'would replace it')
                connection = self.frames.connection
                connection.register(landing, pulled)
                try:
                    if not self._schema_exists(database, table.schema):
                        _host.run(connection, {'op': 'schema', 'database': database, 'name': table.schema})
                        table.made_schema = True
                    _host.run(connection, {'op': 'relation', 'kind': 'table', 'target': target, 'replace': True,
                                           'replacing': [{'column': c['column'], 'sql': c['sql']} for c in conversions],
                                           'from': {'relation': _host.name(landing)}})
                except duckdb.Error as refused:
                    # nothing left of a copy that failed: no table, and no schema it made
                    self._drop_copy(table, database, target)
                    return _said(422, f'the snap could not be copied into this Python\'s DuckDB: {refused}')
                finally:
                    connection.unregister(landing)
                table.holders += 1
                table.frozen = {'rowCount': rows, 'pulledBy': {
                    'plane': 'warehouse', 'where': receipt.get('where', ''), 'as': receipt.get('as', ''),
                    **({'statementId': receipt['statementId']} if 'statementId' in receipt else {}),
                    **({'serverRows': receipt['serverRows']} if 'serverRows' in receipt else {})}}
                said = table.frozen
        return Answer(200, 'application/json', json.dumps(said).encode('utf-8'))

    def _drop_copy(self, table: _WarehouseTable, database: str, target: dict[str, Any]) -> None:
        """A table's copy dropped, and the schema it made when nothing else is in it now."""
        table.frozen = None
        connection = self.frames.connection
        _host.run(connection, {'op': 'drop', 'kind': 'table', 'target': target})
        if table.made_schema:
            if _host.count(connection, {'op': 'tablesIn', 'database': database, 'schema': table.schema}) == 0:
                _host.run(connection, {'op': 'dropSchema', 'database': database, 'name': table.schema})
            table.made_schema = False

    def _table_exists(self, database: str, schema: str, name: str) -> bool:
        return _host.count(self.frames.connection,
                           {'op': 'relationExists', 'name': _host.name(name, schema=schema, database=database)}) > 0

    def _schema_exists(self, database: str, schema: str) -> bool:
        return _host.count(self.frames.connection, {'op': 'schemaExists', 'database': database, 'schema': schema}) > 0

    def _warehouse_table_of(self, body: str) -> _WarehouseTable | None:
        """The warehouse table a plan-and-run's model is the model of, or None."""
        try:
            asked = json.loads(body)
        except ValueError:
            return None
        model = asked.get('model') if isinstance(asked, dict) else None
        if not isinstance(model, str):
            return None
        with self._warehouse_lock:
            return self._warehouse_tables.get(model)

    def _warehouse_plan_and_run(self, table: _WarehouseTable, body: str) -> Answer:
        """PLANANDRUN over a warehouse table (C3): planned by Python's planner against the model it holds, on the
        compiler's threads; against the live runtime its SQL runs on the warehouse as the signed-in person, the
        server's Arrow chunks the rows, typed by the plan as Python's own are; against the snap runtime, on the copy in
        the frames' DuckDB while there is one. The page sends Pure, never SQL (R4). An expired session is 401, its words
        the driver's, with ``expired`` naming the warehouse and the person (the page offers to sign in again)."""
        asked = json.loads(body)
        if not all(isinstance(asked.get(k), str) for k in ('lambda', 'model', 'runtime')):
            return _said(400, 'a plan-and-run is {"lambda", "model", "runtime"}, each a string')
        wrong = table.refused_runtime(asked['runtime'])
        if wrong is not None:
            return _planned({'plan': self.planner.refused_plan(wrong)})
        snapped = asked['runtime'] == table.snap_runtime and table.snapped
        on = _on_planner_threads()
        planned = on(self.planner.plan, asked['lambda'], table.held, asked['runtime'])
        try:
            plan = on(self.planner.read, planned)
        except LegendError:
            return _planned({'plan': planned})
        try:
            receipt: dict[str, Any] | None = None
            if snapped:
                # the copy in the frames' DuckDB, through the snap runtime: Python's executor, as a frame's rows
                rows = self.frames.run(plan)
            else:
                said: dict[str, Any] = {}
                rows = typed_by_plan(_arrow_of(table.warehouse.arrow_chunks(plan.sql, said.update)), plan)
                # the warehouse's own receipt: where, as whom, its statement and its count (the tab's live reads carry it)
                receipt = {'plane': 'warehouse', 'where': said.get('where', ''), 'as': said.get('as', ''),
                           **({'statementId': said['statementId']} if 'statementId' in said else {}),
                           **({'serverRows': said['serverRows']} if 'serverRows' in said else {})}
        except warehouse.SessionExpired as expired:
            return _planned({'plan': planned, 'error': str(expired),
                             'expired': {'baseUrl': expired.base_url, 'principal': expired.principal}}, 401)
        except (warehouse.WarehouseError, UntypedColumn, pa.ArrowInvalid, duckdb.Error) as refused:
            return _planned({'plan': planned, 'error': f'{type(refused).__name__}: {refused}'}, 422)
        return _arrow_rows(rows, planned, receipt)

    def _plan_and_run(self, body: str) -> Answer:
        """PLANANDRUN on Python (§15.2): ``{"lambda", "model", "runtime"}`` -- the plan request the page's planner makes
        (lite-planner.ts ``planRequest``), its model as text -- planned against the model Python holds of that text
        (``Frames.held_for``: only the models this engine serves, R4) by the planner function the tab's worker calls
        (``planJson``), its SQL run on the frames' DuckDB, all in this one call: the page sends Pure, never SQL. The
        answer is the database's rows as Arrow, the planner's answer in their schema's metadata (``legend.plan``),
        which the page reads as its planner reads its own; or, no rows, JSON: the planner's refusal (``plan``), or its
        plan and why its SQL was refused (``error``)."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a plan-and-run is {"lambda", "model", "runtime"}, as JSON')
        if not (isinstance(asked, dict) and all(isinstance(asked.get(k), str) for k in ('lambda', 'model', 'runtime'))):
            return _said(400, 'a plan-and-run is {"lambda", "model", "runtime"}, each a string')
        refused = self.planner.refused_plan('this engine plans only over the tables it serves: a page\'s frames and '
                                            'files, a model that reads only them, and a project\'s model with Data rows')
        with self.frames.serving() as models:
            self._noted(models)
            served = self.frames.model_for(asked['model']) is not None
        if not served:
            return self._project_plan_and_run(asked, refused)
        with self.frames.serving() as models:
            self._noted(models)
            held = self.frames.model_for(asked['model'])
            if held is None:
                return _planned({'plan': refused})
            # Python's own planner, over the model this process holds
            planned = self.planner.plan(asked['lambda'], held, asked['runtime'])
            try:
                plan = self.planner.read(planned)
            except LegendError:
                return _planned({'plan': planned})
            try:
                # Python's executor, as Python's own queries are run: the rows typed by the plan (frames.typed_by_plan)
                rows = self.frames.run(plan)
            except (duckdb.Error, UntypedColumn) as refused:
                return _planned({'plan': planned, 'error': f'{type(refused).__name__}: {refused}'}, 422)
        return _arrow_rows(rows, planned)

    def _project_plan_and_run(self, asked: dict[str, str], refused: str) -> Answer:
        """A plan-and-run over a project's model (a saved query opened on Python's page: C5): planned against
        the model held beside the project's own database, by Python's planner, and run there on its cursor -- the
        page sent Pure; the rows are the project's Data rows. A model with none is refused, as one Python serves no
        table of is."""
        with self.projects.using(asked['model']) as project:
            if project is None:
                return _planned({'plan': refused})
            planned = self.planner.plan(asked['lambda'], project.held, asked['runtime'])
            try:
                plan = self.planner.read(planned)
            except LegendError:
                return _planned({'plan': planned})
            try:
                rows = typed_by_plan(project.cursor.execute(plan.sql).to_arrow_table(), plan)
            except (duckdb.Error, UntypedColumn) as failure:
                return _planned({'plan': planned, 'error': f'{type(failure).__name__}: {failure}'}, 422)
        return _arrow_rows(rows, planned)

    def _plan(self, body: str) -> Answer:
        """THE DRY RUN of a plan-and-run (§15.2 as amended 2026-10-15; the page's compile check, CubeController.compile):
        ``{"lambda", "model", "runtime"}``, as plan-and-run takes it, planned by the same planner function against the
        model Python holds of that text -- a page's frame or file, a warehouse table, a project's model with Data rows
        (C5): only the models this engine serves (R4) -- and never run. JSON: ``{"plan"}``, the planner's answer, a
        refusal too, as plan-and-run carries it (``legend.plan``)."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a plan is {"lambda", "model", "runtime"}, as JSON')
        if not (isinstance(asked, dict) and all(isinstance(asked.get(k), str) for k in ('lambda', 'model', 'runtime'))):
            return _said(400, 'a plan is {"lambda", "model", "runtime"}, each a string')
        refused = self.planner.refused_plan('this engine plans only over the tables it serves: a page\'s frames and '
                                            'files, a model that reads only them, and a project\'s model with Data rows')
        with self.frames.serving() as models:
            self._noted(models)
            held = self.frames.model_for(asked['model'])
            if held is None:
                with self._warehouse_lock:
                    table = self._warehouse_tables.get(asked['model'])
                if table is not None:
                    # the runtime its run takes, refused as its run refuses it
                    wrong = table.refused_runtime(asked['runtime'])
                    if wrong is not None:
                        return _planned({'plan': self.planner.refused_plan(wrong)})
                    held = table.held
            if held is not None:
                return _planned({'plan': self.planner.plan(asked['lambda'], held, asked['runtime'])})
            with self.projects.using(asked['model']) as project:
                if project is None:
                    return _planned({'plan': refused})
                return _planned({'plan': self.planner.plan(asked['lambda'], project.held, asked['runtime'])})

    def _execute(self, body: str) -> Answer:
        """execute in upstream's Arrow format: the compiler plans over the frames' models, DuckDB runs its SQL,
        and the rows go back as upstream writes them. One query at a time (the frames are held still while it
        runs); the answer is compressed outside."""
        with self.frames.serving() as models:
            self._noted(models)
            with self._project_of(body, models) as (served, connection):
                answer = self.planner.execute_plan(body, served)
                if answer.status != 200:
                    return Answer(answer.status, answer.content_type, answer.body.encode('utf-8'))
                planned = json.loads(answer.body)
                rows = connection.execute(planned['sql']).to_arrow_table()
        # upstream's headers for this answer (measured, legend-engine 4.145.0)
        return Answer(200, 'application/json', _arrow_ipc(rows, planned['metadata']),
                      {'x-legend-response-format': 'FormatNotSet'})

    def _execute_json(self, body: str) -> Answer:
        """execute in legend-engine's JSON (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): the compiler plans over the
        frames' models, DuckDB writes the whole result as one value (the JSON wire, as legend-lite's server's database
        writes it), and the compiler arranges it with the server's own writers -- the server's answer text, as the tab's
        is. One query at a time (the frames are held still while it runs)."""
        with self.frames.serving() as models:
            self._noted(models)
            with self._project_of(body, models) as (served, connection):
                planned = self.planner.execute_json_plan(body, served)
                if planned.status != 200:
                    return Answer(planned.status, planned.content_type, planned.body.encode('utf-8'))
                row = connection.execute(json.loads(planned.body)['run']).fetchone()
        wire = row[0] if row is not None and row[0] is not None else ''
        answer = self.planner.execute_json_answer(planned.body, wire)
        return Answer(answer.status, answer.content_type, answer.body.encode('utf-8'))

    @contextlib.contextmanager
    def _project_of(self, body: str, models: dict[str, str]) -> Iterator[tuple[list[str], Any]]:
        """Where an execute runs: over the frames' models, on the frames' connection -- or, for a project's model with
        Data rows (Query's or Studio's, served here: C5), over that model alone, on a cursor of its project's own
        database (C5). A model neither is stays refused by the compiler, as before (a frame's that changed, say)."""
        served = list(models.values())
        model = _request_model(body)
        if model is None or model in served:
            yield served, self.frames.connection
            return
        with self.projects.using(model) as project:
            if project is None:
                yield served, self.frames.connection
            else:
                yield [model], project.cursor

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
        """A page it serves (``serve_page``) as it is open now (the open page's last word on it, at its version):
        ``{"version": n, "page": <its document>}``, or 404."""
        name = parse_qs(query).get('page', [''])[0]
        with self._changes:
            served = self._pages.get(name)
        if served is None:
            return _said(404, f'this engine serves no page named {name!r}')
        return Answer(200, 'application/json', _json.dumps({'version': served[0], 'page': served[1]}).encode('utf-8'))

    def _page_said(self, query: str, body: str) -> Answer:
        """The open page's document as it is now (posted by the page as it changes, with the version it opened):
        kept for ``read_page`` -- unless the page has been served again since, when what it says is of a document
        Python has replaced (409), and the page opens the new one."""
        asked = parse_qs(query)
        name = asked.get('page', [''])[0]
        version = asked.get('version', [''])[0]
        if not version.isdigit():
            return _said(400, 'a page says what it is with the version it opened (version=n)')
        try:
            document = _json.loads(body)
        except ValueError:
            return _said(400, 'a page is its document, as JSON')
        if not isinstance(document, dict) or document.get('kind') != 'datacube.page':
            return _said(400, 'a page is its document (kind datacube.page)')
        with self._changes:
            served = self._pages.get(name)
            if served is None:
                return _said(404, f'this engine serves no page named {name!r}')
            if served[0] != int(version):
                return _said(409, f'the page {name!r} is at version {served[0]}: what version {version} says is replaced')
            self._read[name] = document
            # and it is the page as it is open now: what a page opening on it (another tab, a reload) opens
            self._pages[name] = (served[0], document)
        return Answer(204, 'text/plain; charset=utf-8', b'')

    def read_page(self, name: str) -> dict[str, Any] | None:
        """A page's document as the open page last said it is (its changes made in DataCube included), a copy -- or
        None when it has said nothing since it was served."""
        with self._changes:
            said = self._read.get(name)
        return None if said is None else _json.loads(_json.dumps(said))

    def serve_page(self, name: str, document: dict[str, Any]) -> int:
        """Serves a copy of ``document`` -- DataCube's page document (version 3), each cube over one of this engine's
        frames (``{"_type": "frame", "name": ...}``) -- as the page ``name``; served again, its version moves, and a page
        open on it opens it again. Returns its version."""
        kept = _json.loads(_json.dumps(document))
        with self._changes:
            version = self._page_versions[name] = self._page_versions.get(name, 0) + 1
            self._pages[name] = (version, kept)
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

    def app_config(self, app: str, origin: str) -> Answer:
        """An app's ``config.json`` as this engine serves the app (E): every service it reads at this engine's own
        addresses, ``origin`` the one its page was loaded from -- Query's execution, saved queries and projects by name
        (a Depot), Studio's SDLC, Depot and engine."""
        api, sdlc, depot = f'{origin}/api', f'{origin}/sdlc/api', f'{origin}/depot/api'
        config: dict[str, Any] = (
            {'execution': {'kind': 'server', 'engine': api}, 'projects': [], 'depot': {'sdlc': sdlc, 'depot': depot},
             'vendor': './vendor/'}
            if app == 'query' else
            {'sdlc': sdlc, 'depot': depot, 'vendor': './vendor/', 'worker': './planner-worker.js', 'engine': api})
        return Answer(200, 'application/json', json.dumps(config).encode('utf-8'))

    def _file(self, path: str) -> Answer:
        """A file of the site, by its path under it -- or of an app's (``/query/...``, ``/studio/...``), under its own."""
        app = _app_of(path)
        if app is not None:
            relative = unquote(path[len(app) + 2:])
            site = _app_site(app)
        else:
            relative = 'index.html' if path == '/' else unquote(path).lstrip('/')
            site = self.site
        parts = relative.split('/')
        file = site / relative if site is not None else None
        # the path as written, never resolved: a built site's files are links into the build's output (Bazel's), as
        # the warehouse serves them (WarehouseServer.site)
        # a part holding ':' would be a drive on Windows ('C:/x' replaces the root), one holding NUL no file name
        if (file is None or '\\' in relative or '..' in parts or '' in parts
                or any(':' in part or '\0' in part for part in parts) or not file.is_file()):
            return _said(404, f'no such path: {path}')
        return Answer(200, _site_types().get(file.suffix, 'application/octet-stream'), file.read_bytes())

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
    # the connections waiting to be accepted, at most (listen's backlog): socketserver's default is 5, and macOS resets a
    # connection the queue has no room for -- a burst (a page's tabs and widgets opening together, a browser opening six
    # to an origin each) on a busy machine lost one (CI run 38018795800, macOS). The systems' usual ceiling instead
    request_queue_size = 128

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
        if private(path) and not (hmac.compare_digest(self.headers.get('Authorization', '').encode('utf-8'), web._authorization)
                                  or self._carries_cookie()):
            self._refuse_unread(401, "this engine answers its own page only: the request did not carry the engine's token")
            return False
        return True

    def _carries_cookie(self) -> bool:
        """Whether the request carries the cookie an app's page holds the engine's token in (E), sent by that page
        itself. A browser sends a host's cookies to its every port, SameSite or not (a site is a host, not an origin),
        so a page of any other server on this machine would send it too: the cookie admits a call only when the browser
        says the call is the page's own (``Sec-Fetch-Site: same-origin``), or, a browser that says nothing of it, when
        its ``Origin`` is this engine's. The header is read by hand: it carries every local server's cookies, and
        ``http.cookies`` drops the whole header at the first value it cannot read."""
        web = self.server.web
        token = web.token.encode('utf-8')
        held = False
        for part in self.headers.get('Cookie', '').split(';'):
            name, eq, value = part.partition('=')
            # each one compared: another server on this machine may have set one of the same name
            if eq and name.strip() == web.cookie and hmac.compare_digest(value.strip().encode('utf-8'), token):
                held = True
        if not held:
            return False
        site = self.headers.get('Sec-Fetch-Site')
        if site is not None:
            return site == 'same-origin'
        return self.headers.get('Origin') == f'http://{self.headers.get("Host")}'

    def do_POST(self) -> None:  # noqa: N802 -- the stdlib's name
        url = urlsplit(self.path)
        # the API, an open page saying what it is now (page.json: Python's page.read()), a page's file in or out, the
        # saved pages, the page's settings, and PlanAndRun's calls
        if not (url.path.startswith(_API) or url.path.startswith(_LITE_API) or _model_home(url.path) or url.path in (
                '/page.json', _FILE_IN, _FILE_PART, _FILE_OUT, _REMOTE, _SETTINGS, _PLAN_AND_RUN, _PLAN, _WAREHOUSE)):
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
        app = _app_of(url.path)
        if app is not None:
            if not self._admitted(url.path):
                return
            self.server.answering(self.connection)
            token = parse_qs(url.query).get('token')
            if token is not None:
                # the app's first load (ll.query(), ll.studio()): the link's token exchanged for the cookie every call of
                # the page then carries, and the address without it
                if not hmac.compare_digest(token[0].encode('utf-8'), self.server.web.token.encode('utf-8')):
                    self._send(_said(401, "this engine answers its own page only: the link did not carry the engine's token"))
                    return
                rest = urlencode([(k, v) for k, v in parse_qsl(url.query, keep_blank_values=True) if k != 'token'])
                self._send(Answer(303, 'text/plain; charset=utf-8', b'', {
                    'Location': f'{url.path}?{rest}' if rest else url.path,
                    'Set-Cookie': f'{self.server.web.cookie}={self.server.web.token}; Path=/; HttpOnly; SameSite=Strict',
                }))
                return
            if url.path == f'/{app}/demo/config.json':
                self._send(self.server.web.engine.app_config(app, f'http://{self.headers.get("Host")}'))
                return
            self._send(self.server.web.engine.answer('GET', url.path, url.query, None))
            return
        if self._admitted(url.path):
            self.server.answering(self.connection)
            self._send(self.server.web.engine.answer('GET', url.path, url.query, None))

    def do_PUT(self) -> None:  # noqa: N802
        """The query store's PUTs (a saved query updated or patched), with the token."""
        url = urlsplit(self.path)
        if not _saved_query(url.path):
            self._refuse_unread(405, 'this engine answers PUT for its query store only')
            return
        self._with_body('PUT', url)

    def do_DELETE(self) -> None:  # noqa: N802
        """The model home's DELETEs (the SDLC's: a workspace, a review) and the query store's (a saved query), with the
        token; its body, when it has one."""
        url = urlsplit(self.path)
        if not (_model_home(url.path) or _saved_query(url.path)):
            self._refuse_unread(405, 'this engine answers DELETE for its model home and its query store only')
            return
        self._with_body('DELETE', url)

    def _with_body(self, method: str, url: Any) -> None:
        """A call that may carry a body (PUT, DELETE), answered by the engine once its body is read."""
        if not self._admitted(url.path):
            return
        length = self._length() or 0
        if length > _MAX_BODY:
            self._send(_said(413, f'a request body is at most {_MAX_BODY} bytes'))
            return
        try:
            raw = self.rfile.read(length) if length else b''
        except OSError:
            return
        self.server.answering(self.connection)
        try:
            body = raw.decode('utf-8')
        except UnicodeDecodeError:
            self._send(_said(400, 'a request body is UTF-8'))
            return
        self._send(self.server.web.engine.answer(method, url.path, url.query, body))

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
        self.cookie = f'legend-lite-{port}'
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
