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
import hashlib
import hmac
import http.server
import json
import os
import re
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
from .frames import Frames, Reading, UntypedColumn, typed_by_plan
from .keeper import PageKeeper, Refused, Settled
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
# a grid's Snap on a page Python serves (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): its base query copied into a
# table of the frames' database of its own -- a frame's, a page's file's, or a warehouse table's -- and let go of
_SNAP = '/snap.json'
# the id a page open on another asks by (follower=): a tab's or a notebook widget's, made by DataCube's code
_FOLLOWER = re.compile(r'[A-Za-z0-9-]{1,64}')
# a saved query's rows are its project's own Data rows (C5): one whose project has none is refused in these words
_NO_PROJECT_ROWS = 'a saved query\'s rows are its project\'s own Data rows: this model has none'
# a saved grid of a page Python serves, opened by Python over its frames as they are now (§18.3): a join of frames, which
# the page cannot open itself -- it has no compiler and no database of its own on Python's page
_OPENED = '/opened.json'
# a join of two of a page's grids asked by a tab on a page Python serves, which has no database of its own here
# (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B11): its two sides, what it will do, its grid -- each DataCube's own code's,
# headless, over the tables Python serves, as page.join asks it
_JOIN = '/join.json'
# a new grid's cube over a class asked by a tab on a page Python serves, which has no compiler or model home of its own
# here (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md K16, K17): made by DataCube's own code, headless, as page.grid makes one
_SOURCE = '/source.json'
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


def _snap_refused(headless: Any, rows: int) -> Answer:
    """A copy of ``rows`` rows past this machine's limit (N3: each host its own), refused in the tab's own sentence
    (headless snapRefused: snap.ts snapRefusal)."""
    return _said(422, headless.call('snapRefused', str(rows), str(_MAX_SNAP_ROWS)))


def _arrow_of(chunks: Iterable[bytes]) -> pa.Table:
    """A result's Arrow IPC streams, one per chunk, as one table: the first chunk's schema (every chunk carries it;
    the warehouse's DuckDB writes one at least, its schema alone for no rows)."""
    tables = [pyarrow.ipc.open_stream(c).read_all() for c in chunks]
    if not tables:
        raise pa.ArrowInvalid('the warehouse answered no chunk, not even its columns')
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def _on_planner_threads() -> Callable[..., Any]:
    """Where a planner's calls run: the compiler's threads (the native library's) -- in place on one of them, DataCube's
    engine's among them (_pool.library_thread: D15)."""
    return lambda f, *a: _on_compiler_threads(lambda: f(*a))


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


def _bytes_of(path: Path) -> tuple[int, str]:
    """A file's size and the lower-case hex SHA-256 of its bytes: what a page names a file by (cube-document.ts
    FileSource.sha256), the digest the host computes for DataCube (as its ``digest`` call does), read in parts."""
    digest = hashlib.sha256()
    with path.open('rb') as file:
        for part in iter(lambda: file.read(1 << 20), b''):
            digest.update(part)
    return path.stat().st_size, digest.hexdigest()


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
        _PLAN_AND_RUN, _PLAN, _WAREHOUSE, _SNAP, _OPENED, _JOIN, _SOURCE)


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
    its plans made against its runtime and run on the warehouse, through the binding. A grid over it snaps into a copy
    of its own (``Engine._warehouse_snap``), never one under the table's name (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
    §18.3)."""

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

    @property
    def runtime(self) -> str:
        return self.written['runtime']

    def refused_runtime(self, runtime: str) -> str | None:
        """Why a query of this table may not name ``runtime``, or None: its own runtime, on the warehouse (a grid's copy
        is read through Python's own model of it). Asked by its plan-and-run and its dry run alike (``plan.json``)."""
        if runtime == self.runtime:
            return None
        return f'{self.schema}.{self.name} is read on its warehouse, through its own runtime'

    def reads(self, base_url: str, catalog: str) -> bool:
        return self.warehouse.base_url.rstrip('/') == base_url.rstrip('/') and self.catalog == catalog


class Engine:
    """legend-engine's ``pure/v1`` API over the frames' tables, with no transport: ``answer(method, path, query,
    body)`` is one call, answered as HTTP would carry it. ``site``: a directory whose files are answered too
    (DataCube's built pages). ``WebServer`` serves it to a browser tab; a notebook's cube sends it its widget's
    messages."""

    def __init__(self, frames: Frames, site: str | Path | None = None,
                 headless: Callable[[], Any] | None = None, pages: str | Path | None = None,
                 keeper: PageKeeper | None = None) -> None:
        """``headless``: DataCube's own non-screen code over the frames' database (legend_lite._headless), made when
        first asked for -- what reads a page's file into it (file.json). Absent, the engine takes no file, and says so
        (services.json). ``pages``: the folder the page's Save and Open… keep pages in (step 2d, B6): this user's
        (``home()/pages``) when not given. ``keeper``: the one copy of each page it serves (keeper.py), its session's,
        which Python's pages write too; one of its own when not given."""
        self.frames = frames
        self.keeper = keeper if keeper is not None else PageKeeper()
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
        # the copies grids snapped (each its own table of the frames' database, served as a page's file is), each with
        # its model, runtime and relation as DataCube's code modelled it as it landed: only these are let go of by a
        # snap's release
        self._copies: dict[str, dict[str, Any]] = {}
        # the copies let go of already, and how many were made: a copy's name is never given again in this engine's life
        # (snap, snap_2, ...), so a release that comes late -- a tab's closing grid's, after its page's other editor let
        # the copy go (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8) -- finds it gone, never another grid's copy
        self._released: set[str] = set()
        self._copies_made = 0
        self._warehouse_lock = threading.Lock()
        # the binding's calls on the compiler's threads, never a connection's (warehouse.set_library_thread)
        warehouse.set_library_thread(_on_compiler_threads)
        self.planner = NativePlanner()
        # a project's own rows, when Query or Studio served here runs one of its queries (C5): each project
        # version in a database of its own inside the frames' DuckDB, made by the native library's statements
        self.projects = ProjectDatabases(frames.connection, guard=frames.using_connection)
        self._pages_folder = Path(pages) if pages is not None else None
        self.site = Path(site).resolve() if site is not None else None
        if self.site is not None and not self.site.is_dir():
            raise ValueError(f'the site {site} is not a directory')
        # each frame's version: bumped when it changes (changed), so a page asking sees it and reads it again; and the
        # model each was last served with, so a frame whose columns changed is noticed (_noted)
        self._versions: dict[str, int] = {}
        self._models: dict[str, str] = {}
        self._watchers: list[Callable[[str, int], None]] = []
        self._changes = threading.Lock()

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
            if path == _SNAP:
                return self._snap_said(body or '')
            if path == _OPENED:
                return _workers().submit(self._opened_said, body or '').result()
            if path == _JOIN:
                return _workers().submit(self._join_said, body or '').result()
            if path == _SOURCE:
                return _workers().submit(self._source_said, body or '').result()
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

    def answer_over(self, path: str, body: str, over: Reading) -> tuple[int, str, bytes]:
        """A call of DataCube's code run headless for a grid of a page in Python (headless calls.ts ``engine``), answered
        by this engine's own code over the read of the frames its call was made in (``over``: Frames.reading, D15), on
        whichever thread asks -- DataCube's own, or Python's for a grid's rows -- holding nothing a moment longer than it
        plans: ``/plan-and-run.json`` (a grid's query planned and run where its rows are: the frames' database as the read
        sees it, a project's own, a warehouse) and ``/warehouse.json`` (a warehouse table opened under the session this
        engine holds: D13). Any other path refused. Its status, media type, body."""
        if path == _PLAN_AND_RUN:
            table = self._warehouse_table_of(body)
            said = (_answered(lambda b: self._warehouse_plan_and_run(table, b), body) if table is not None
                    else _answered(lambda b: self._plan_and_run(b, over), body))
        elif path == _WAREHOUSE:
            said = _answered(self._warehouse, body)
        else:
            said = _said(404, f'DataCube asks the engine to plan and run ({_PLAN_AND_RUN}) or open a warehouse table '
                              f'({_WAREHOUSE}), not {path}')
        return said.status, said.content_type, said.body

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
        from ._home import home_call
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
        try:
            said = self._mount(url, asked['name'], asked.get('s3'))
        except ValueError as refused:
            return _said(400, str(refused))
        except Exception as failure:
            # the file not read (not there, its keys refused, a format DuckDB cannot read): said, as the tab says it
            return _said(422, f'{type(failure).__name__}: {failure}')
        return Answer(200, 'application/json', json.dumps(said).encode('utf-8'))

    def _mount(self, url: str, asked: str, s3: Mapping[str, Any] | None) -> dict[str, Any]:
        """A remote file mounted as a view of its own, named ``asked`` when it is free (else numbered), by DataCube's own
        code: its extensions and keys first, then -- an Iceberg table -- every file it names checked remote, before any
        view of it reads it; kept by its URL (its ``leaf``) from then on. DataCube's answer: its model, runtime, accessor
        and the file it is. A URL or an engine that reads no remote file refused in words (``ValueError``)."""
        if not remote_url(url):
            raise ValueError(f'a remote file is read from {", ".join(s + "://" for s in _REMOTE_SCHEMES)}, never from '
                             f'this machine: {url!r} is not such a URL (a file of yours: give its path)')
        refused = self.remote_refusal()
        if refused is not None:
            raise ValueError(f'this engine reads no remote file: {refused}')
        extensions = duckdb_extensions()
        assert extensions is not None
        assert self._headless is not None
        # asked for before the frames are held (the session's lock first, then the frames')
        headless = self._headless()
        # its turn first, then the frames held: no one holds the frames waiting for a turn (D15)
        with headless.turn(), self.frames.reading_file(asked, view=True) as name:
            self._extensions_here(extensions)
            mount = {'url': url, 'name': name, **({'s3': dict(s3)} if s3 else {})}
            # the source's extensions and keys first, then what it names checked, before any view of it reads it
            scan = json.loads(headless.call('remoteAccess', json.dumps(mount)))
            if scan['format'] == 'iceberg':
                refused = self._iceberg_refusal(scan['url'])
                if refused is not None:
                    raise ValueError(refused)
            said: dict[str, Any] = json.loads(headless.call('mount', json.dumps(mount)))
            self.frames.keep_file(name, said, view=True)
        return {**said, 'table': name}

    def mount_remote(self, url: str, s3: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """A page's remote file mounted again by its URL (``Page.load``: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D14),
        under the view DataCube names one (headless tableFor), as the tab's mount does (``_mount``). One not read (not
        there, its keys refused, a format DuckDB cannot read) refused in words (``ValueError``), as the tab says it."""
        assert self._headless is not None
        try:
            return self._mount(url, self._headless().call('tableFor', None), s3)
        except ValueError:
            raise
        except Exception as failure:
            raise ValueError(f'the remote file {url} is not read: {type(failure).__name__}: {failure}') from failure

    def mount_remote_once(self, url: str) -> tuple[dict[str, Any], bool]:
        """A remote file mounted for a grid (``page.grid(remote=...)``), once for its URL: the view a remote file of the
        same URL is read through already, as this engine serves it, or the file mounted now (``mount_remote``) -- and
        whether it was mounted now."""
        held = self.frames.hosted()['remotes'].get(url)
        if held is not None:
            return held, False
        return self.mount_remote(url), True

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
            said = json.dumps(self._read_in(path, asked['name'], asked['table']))
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

    def _read_in(self, path: Path, name: str, asked: str) -> dict[str, Any]:
        """A file read into a table of its own, named ``asked`` when it is free (else the next free name: the answer says
        which), by DataCube's own code (headless ingest, upload.ts readFileInto), told its bytes' size and hash -- by which
        a page names it (``_bytes_of``) -- and kept by them from then on. DataCube's answer: what arrived (its model,
        runtime, accessor, table and rows) and the file it is (its ``leaf``)."""
        assert self._headless is not None
        # asked for before the frames are held: the session takes its own lock first, then the frames' (register)
        headless = self._headless()
        size, sha256 = _bytes_of(path)
        # its turn first, then the frames held: no one holds the frames waiting for a turn (D15)
        with headless.turn(), self.frames.reading_file(asked) as table:
            read: dict[str, Any] = json.loads(headless.call('ingest', json.dumps(
                {'path': str(path), 'name': name, 'table': table, 'size': size, 'sha256': sha256})))
            self.frames.keep_file(table, read)
        return read

    def read_file(self, path: str | Path, name: str) -> dict[str, Any]:
        """A page's file read in from ``path`` under ``name`` (``Page.load``'s files: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
        D14), into the table DataCube names one (headless tableFor), as the tab's file is read (``_read_in``). The file is
        the caller's: it is read, never moved or deleted. An engine that reads no file refused in words (``ValueError``)."""
        if not self.takes_files():
            raise ValueError('this engine reads no file: legend-lite reads one with DataCube\'s own code, headless, which '
                             'this engine was not given')
        assert self._headless is not None
        return self._read_in(Path(path), name, self._headless().call('tableFor', name))

    def read_file_once(self, path: str | Path, name: str) -> tuple[dict[str, Any], bool]:
        """A file read in for a grid (``page.grid(file=...)``: X4), once for its bytes: the table a file of the same
        bytes was read into already, as this engine serves it, or the file read in now (``read_file``) -- and whether it
        was read now."""
        size, sha256 = _bytes_of(Path(path))
        held = self.frames.hosted()['files'].get(sha256)
        if held is not None:
            return held, False
        return self.read_file(path, name), True

    def forget_read(self, read: Mapping[str, Any]) -> None:
        """A file or remote file read in for a grid that was refused (``read``: as DataCube read it): let go of at once,
        nothing reading it -- a remote file's view dropped as a view."""
        assert self._headless is not None
        headless = self._headless()
        with headless.turn(), self.frames.holding():
            headless.call('forget', json.dumps({'table': read['table'], 'view': 'url' in read['leaf']}))
            self.frames.forget_file(read['table'])

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
        with self._warehouse_lock:
            # a grid's snapped copy is served as a file's table is, and let go of only by its snap's release
            copy = table in self._copies
        if table is None or copy or not self.takes_files():
            return _said(404, f'{asked["table"]!r} is no table a page\'s file was read into')
        # a page the keeper keeps reads it: the keeper's from then on, it goes when nothing reads it any more (sweep: X3)
        sha256 = next((key for key, held in self.frames.read_files().items() if held == table), None)
        if sha256 is not None and self.keeper.has_read(sha256, 'files'):
            return Answer(200, 'application/json', b'{"kept": "a page reads it: it goes when nothing reads it any more"}')
        assert self._headless is not None
        try:
            # asked for before the frames are held (the session's lock first, then the frames')
            headless = self._headless()
            with headless.turn(), self.frames.holding():
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
        written by the tab's own writer (``table_model``: read-only) and held, answered as that writer answers it. A
        grid over it snaps through ``snap.json``, as any grid on the page does. A refusal is its words; an
        expired session 401, a warehouse's refusal 422."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a warehouse call is JSON')
        if not isinstance(asked, dict) or not isinstance(asked.get('op'), str):
            return _said(400, 'a warehouse call is {"op", ...}')
        try:
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
                'columns': [{**c, 'dataType': c['type']} for c in found['columns']]}
        # on the compiler's threads -- in place on one, DataCube's thread among them (F29: never waiting on the pool)
        written = _on_compiler_threads(lambda: compiler.table_model(spec))
        if self.frames.held_for(written['model']) is not None:
            # a frame or a page's file whose model is this text: the warehouse's would answer for it
            return _said(409, f'a frame or file of this Python is {schema}.{name} with the same columns: rename one')
        held = _on_compiler_threads(lambda: compiler.HeldModel(written['model']))
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
            _on_compiler_threads(spare.close)
        if refusal is not None:
            return _said(409, refusal)
        said = {k: v for k, v in written.items() if k != 'accessor'}
        return Answer(200, 'application/json', json.dumps(said).encode('utf-8'))

    def _snap_said(self, body: str) -> Answer:
        """SNAP, ON PYTHON (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3; engine-client/src/snap.ts, the tab's, is its
        twin): a grid's base query copied into a table of the frames' database of its own (``snap``, ``snap_2``, ...;
        never a source's name, so no two grids share one), made to hold what its model declares and modelled by
        DataCube's own code as the tab models its copy (upload.ts modelLanded, headless), and served from then on as a
        page's file is. ``{"op": "snap", "model", "runtime", "pull"}``: the pull, Pure (the base query, its every
        column), planned over ``model`` and counted where the rows are against the same limit -- on the warehouse, for a
        warehouse table this engine opened (C3; its HTTP on the caller's thread), else in the frames' database, over a
        model this engine serves (a frame's, a page's file's: on the compiler's threads, the frames held still). The
        answer: its count, the pull's receipt (a warehouse's), its table, and its model -- the one its queries are
        planned over. ``{"op": "release", "table"}``: that copy dropped by DataCube's code and served no more -- but one a
        page the keeper keeps reads, kept (it goes with the last page that reads it: ``sweep``), and one let go already,
        answered as let go; a table that
        is no copy a snap made is refused. ``{"op": "held", "table"}``: a copy this engine holds, as it was made --
        its table and model -- for a page following a snap made in Python (the page's snap command names only its table:
        docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8); one it does not hold, 404. A refusal is its words; an expired
        session 401."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a snap is JSON')
        if not isinstance(asked, dict) or asked.get('op') not in ('snap', 'release', 'held'):
            return _said(400, 'a snap is {"op": "snap" | "release" | "held", ...}')
        if not self.takes_files():
            return _said(404, 'this engine keeps no copy: legend-lite models one with DataCube\'s own code, headless, '
                              'which this engine was not given')
        assert self._headless is not None
        if asked['op'] == 'release':
            return self._release_copy(asked.get('table'))
        if asked['op'] == 'held':
            table = asked.get('table')
            made = self.copy_of(table) if isinstance(table, str) else None
            if made is None:
                return _said(404, f'{table!r} is no copy this engine holds')
            said = {**{k: v for k, v in made.items() if k != 'accessor'}, 'table': table}
            return Answer(200, 'application/json', json.dumps(said).encode('utf-8'))
        if not all(isinstance(asked.get(k), str) for k in ('model', 'runtime', 'pull')):
            return _said(400, 'a snap is {"model", "runtime", "pull"}, the pull its lambda as JSON')
        with self._warehouse_lock:
            table = self._warehouse_tables.get(asked['model'])
        if table is not None:
            try:
                return self._warehouse_snap(table, asked)
            except warehouse.SessionExpired as expired:
                return _said(401, str(expired))
        return _workers().submit(self._served_snap, asked).result()

    def _opened_said(self, body: str) -> Answer:
        """A SAVED GRID OPENED BY PYTHON for a page it serves (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3):
        ``{"cube", "page", "gridsOnPage"}`` -- the cube document's text, its page's name, how many grids the page holds --
        opened over the frames as they are now by DataCube's own code, headless (gridOpened: a join of frames opened as
        the tab opens a saved join), in one read of them (D15). The answer is gridOpened's -- the cube rebuilt, what it
        said, and its base: its model, its runtime, and a frame's accessor or a join's relation, which the page reads its
        rows over through Python -- and the version of each frame it reads, which the page follows. A cube over anything
        but this engine's frames is refused in DataCube's words."""
        try:
            asked = _json.loads(body)
        except ValueError:
            return _said(400, 'an opened grid is JSON')
        if not isinstance(asked, dict) or not isinstance(asked.get('cube'), str) or not isinstance(asked.get('page'), str) \
                or not isinstance(asked.get('gridsOnPage'), int):
            return _said(400, 'an opened grid is {"cube": its document\'s text, "page", "gridsOnPage"}')
        if self._headless is None:
            return _said(404, 'this engine opens no saved grid: legend-lite opens one with DataCube\'s own code, headless, '
                              'which this engine was not given')
        try:
            # asked for before the frames are held (the session's lock first, then the frames')
            headless = self._headless()
        except Exception as failure:
            return _said(422, f'{type(failure).__name__}: {failure}')
        # one read of the frames (D15): DataCube's code reads them, and asks the engine, as they were when it began
        with self.frames.reading() as over:
            try:
                made = _json.loads(headless.call('gridOpened', asked['cube'], over.hosted, asked['page'],
                                                 asked['gridsOnPage']))
                leaves = json.loads(headless.call('cubeLeaves', asked['cube']))
            except Exception as refused:
                # DataCube's refusal (a frame not served, a join that no longer joins), in its words, from V8
                return _said(422, f'{type(refused).__name__}: {refused}')
        read = sorted({leaf['name'] for leaf in leaves if leaf['_type'] == 'frame'})
        answer = {**made, 'frames': {name: self.version(name) for name in read}}
        return Answer(200, 'application/json', _json.dumps(answer).encode('utf-8'))

    def _source_said(self, body: str) -> Answer:
        """A NEW GRID'S CUBE OVER A CLASS for a tab on a page Python serves, which has no compiler or model home of its own
        here (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md K16, K17): ``{"model", "given", "page", "gridsOnPage"}`` -- the
        page's model (its text, or null), the class as DataCube names one (its text: ``{class, project?, revision?}``),
        the page's name, how many grids it holds with this one -- made by DataCube's own code, headless (sourceCube, as
        ``page.grid(cls=...)`` makes one), where this host reads the class. The answer is ``{cube}``, its document's
        text, which the tab then opens as it opens every grid of Python's page (``opened.json``); a class refused in
        DataCube's words."""
        try:
            asked = _json.loads(body)
        except ValueError:
            return _said(400, 'a new grid is JSON')
        if not isinstance(asked, dict) or not (asked.get('model') is None or isinstance(asked.get('model'), str)) \
                or not isinstance(asked.get('given'), str) or not isinstance(asked.get('page'), str) \
                or not isinstance(asked.get('gridsOnPage'), int):
            return _said(400, 'a new grid is {"model": the page\'s model\'s text or null, "given": the class\'s text, "page", '
                              '"gridsOnPage"}')
        if self._headless is None:
            return _said(404, 'this engine makes no grid: legend-lite makes one with DataCube\'s own code, headless, which '
                              'this engine was not given')
        try:
            # asked for before the frames are held (the session's lock first, then the frames')
            headless = self._headless()
        except Exception as failure:
            return _said(422, f'{type(failure).__name__}: {failure}')
        with self.frames.reading() as over:
            try:
                made = _json.loads(headless.call('sourceCube', asked['model'], asked['given'], over.hosted, asked['page'],
                                                 asked['gridsOnPage'], 'Python'))
            except Exception as refused:
                return _said(422, f'{type(refused).__name__}: {refused}')
        if 'problems' in made:
            return _said(422, '; '.join(made['problems']))
        return Answer(200, 'application/json', _json.dumps({'cube': made['cube']}).encode('utf-8'))

    def _join_said(self, body: str) -> Answer:
        """A JOIN OF TWO OF A PAGE'S GRIDS for a tab on a page Python serves, which has no database of its own here
        (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B11): ``{"op", "page", "mine", "theirs", "how", "sheet"}`` -- the page
        as the tab holds it (its document's text), the grids joined by their tiles' ids, how they join (not asked of
        ``sides``), the sheet a join's grid goes on (``cube``'s alone; null, its first input's) -- answered by DataCube's own code, headless, in one
        read of the frames and the files and remote files this engine read in (D15), as page.join asks it: ``sides``
        (joinSides: each side's relation and columns, and what drifted), ``questions`` (joinQuestions: what it will do)
        and ``cube`` (joinCube: the addGrid command of its grid, which the tab opens as a saved join of the page). Read
        and written exactly (a condition's decimal keeps its digits); refused in DataCube's words."""
        try:
            asked = _json.loads(body)
        except ValueError:
            return _said(400, 'a join is JSON')
        shape = 'a join is {"op": "sides" | "questions" | "cube", "page": its document\'s text, "mine", "theirs", ' \
                '"how" (but sides), "sheet" (cube)}'
        if not isinstance(asked, dict) or asked.get('op') not in ('sides', 'questions', 'cube') \
                or not all(isinstance(asked.get(k), str) for k in ('page', 'mine', 'theirs')):
            return _said(400, shape)
        op = asked['op']
        if (op != 'sides' and 'how' not in asked) or (op == 'cube' and not isinstance(asked.get('sheet', 0), (str, type(None)))):
            return _said(400, shape)
        if self._headless is None:
            return _said(404, 'this engine makes no join: legend-lite makes one with DataCube\'s own code, headless, '
                              'which this engine was not given')
        try:
            # asked for before the frames are held (the session's lock first, then the frames')
            headless = self._headless()
        except Exception as failure:
            return _said(422, f'{type(failure).__name__}: {failure}')
        spec = {'page': asked['page'], 'mine': asked['mine'], 'theirs': asked['theirs']}
        # one read of the frames (D15): DataCube's code reads them, and asks the engine, as they were when it began
        with self.frames.reading() as over:
            spec['host'] = over.hosted
            try:
                if op == 'sides':
                    said = headless.call('joinSides', _json.dumps(spec))
                elif op == 'questions':
                    said = headless.call('joinQuestions', _json.dumps({**spec, 'how': asked['how']}))
                else:
                    said = headless.call('joinCube', _json.dumps({**spec, 'how': asked['how'], 'sheet': asked['sheet'],
                                                                  'title': None}))
            except Exception as refused:
                # DataCube's refusal (a grid not on the page, a leaf read elsewhere, a condition the compiler refuses),
                # in its words, from V8
                return _said(422, f'{type(refused).__name__}: {refused}')
        return Answer(200, 'application/json', said.encode('utf-8'))

    def snap_copy(self, model: str, runtime: str, pull: str) -> dict[str, Any]:
        """A GRID'S BASE QUERY COPIED (Snap, docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8), for a page in Python: the
        one way ``snap.json`` copies it (``pull``, the protocol's JSON of DataCube's pull, planned over ``model`` and run
        where the rows are -- a warehouse table's on the warehouse), called from Python. Its answer -- the copy's table,
        rows, model, runtime, relation, and the pull's receipt when it ran elsewhere -- or refused in its words
        (ValueError; an expired warehouse session, PermissionError)."""
        answer = self._snap_said(json.dumps({'op': 'snap', 'model': model, 'runtime': runtime, 'pull': pull}))
        if answer.status == 401:
            raise PermissionError(answer.body.decode('utf-8'))
        if answer.status != 200:
            raise ValueError(answer.body.decode('utf-8'))
        # its maker's until a page the keeper keeps reads it (keeper.py has_read)
        return _json.loads(answer.body.decode('utf-8'))

    def release_copy(self, table: str) -> None:
        """A copy Python's snap made let go of by its maker, as ``snap.json``'s release lets one go (its page never read
        it: the write failed); refused in its words (ValueError)."""
        answer = self._release_copy(table)
        if answer.status != 200:
            raise ValueError(answer.body.decode('utf-8'))

    def sweep(self) -> None:
        """EACH COPY GOES WITH THE LAST PAGE THAT READS IT (keeper.py, docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17): the
        copies the keeper says nothing reads any more -- gone live, snapped again, its grid removed, its page collected,
        and every page open on it moved past them -- let go. Called after a page's every write, by whoever wrote it, and
        as an open page says it moved on, holding nothing."""
        with self._warehouse_lock:
            held = list(self._copies)
        for table in self.keeper.letting_go(held):
            # a drop that fails leaves it served, as any copy whose drop failed, tried again at the next sweep
            self._drop_copy(table)
        # and each page's file a kept page read, by the same rule (X3: a remote file's view stays while the engine does)
        files = self.frames.read_files()
        for sha256 in self.keeper.letting_go(files, 'files'):
            self._drop_file(sha256, files[sha256])

    def _drop_file(self, sha256: str, table: str) -> None:
        """A page's file no page reads any more: its table dropped by DataCube's code, and only then served no more; a
        drop that fails leaves it served, tried again at the next sweep."""
        assert self._headless is not None
        try:
            headless = self._headless()
            with headless.turn(), self.frames.holding():
                headless.call('forget', json.dumps({'table': table}))
                self.frames.forget_file(table)
        except Exception:
            traceback.print_exc()
            return
        self.keeper.gone(sha256, 'files')

    def copy_of(self, table: str) -> dict[str, Any] | None:
        """A snap's copy this engine holds, by its table: its model, runtime and relation (a grid snapped onto it reads
        them), or None. It takes no session lock, only this engine's own."""
        with self._warehouse_lock:
            return self._copies.get(table)

    def held_copies(self) -> dict[str, dict[str, Any]]:
        """Every copy this engine holds, by its table: its model, runtime and relation, as DataCube's pageRebase takes
        them (headless page.ts HeldCopy)."""
        with self._warehouse_lock:
            return {t: {'model': c['model'], 'runtime': c['runtime'], 'source': c['source']} for t, c in self._copies.items()}

    def _release_copy(self, name: Any) -> Answer:
        """A snap's copy let go of by its maker (a tab's grid, Python's snap): dropped by DataCube's code, and only then
        served no more -- or, a page the keeper keeps has read it, kept: the keeper's from then on, it goes when nothing
        reads it any more (``sweep``). One let go already is gone: said as let go."""
        with self._warehouse_lock:
            ours = isinstance(name, str) and name in self._copies
            gone = isinstance(name, str) and name in self._released
        if gone:
            return Answer(200, 'application/json', b'{}')
        if ours and self.keeper.has_read(name):
            return Answer(200, 'application/json', b'{"kept": "a page reads it: it goes when nothing reads it any more"}')
        if not ours:
            return _said(404, f'{name!r} is no copy a snap of this engine made')
        return self._drop_copy(name)

    def _drop_copy(self, name: str) -> Answer:
        """A snap's copy dropped by DataCube's code, and only then served no more; its name never given again. One let go
        already (by a sweep at once) is gone."""
        assert self._headless is not None
        with self._warehouse_lock:
            if name not in self._copies:
                return Answer(200, 'application/json', b'{}')
        try:
            # asked for before the frames are held (the session's lock first, then the frames')
            headless = self._headless()
            with headless.turn(), self.frames.holding():
                # dropped first, and only then served no more: a drop that fails leaves it served, to let go again
                headless.call('forget', json.dumps({'table': name}))
                self.frames.forget_file(name)
        except Exception as failure:
            return _said(422, f'{type(failure).__name__}: {failure}')
        with self._warehouse_lock:
            self._copies.pop(name, None)
            self._released.add(name)
        self.keeper.gone(name)
        return Answer(200, 'application/json', b'{}')

    def _landed(self, headless: Any, land: Callable[[Any, dict[str, Any]], None], rows: int,
                receipt: dict[str, Any] | None) -> Answer:
        """A copy made: ``land`` puts the rows into the table named (on the frames' connection, held), DataCube's code
        (``headless``, asked for before the frames were held: the session's lock first, then the frames') models it,
        and it is served from then on; a copy that fails leaves nothing under its name."""
        with self._warehouse_lock:
            self._copies_made += 1
            asked = 'snap' if self._copies_made == 1 else f'snap_{self._copies_made}'
        try:
            # its turn first, then the frames held: no one holds the frames waiting for a turn (D15)
            with headless.turn(), self.frames.reading_file(asked) as name:
                land(self.frames.connection, _host.name(name, schema='main'))
                made = json.loads(headless.call('landed', json.dumps({'table': name})))
                self.frames.keep_file(name, made)
                # a copy from the moment it is served, before the frames are let go: no page's file forget drops it
                with self._warehouse_lock:
                    self._copies[name] = made
        except Exception as failure:
            return _said(422, f'the snap could not be copied into this Python\'s DuckDB: {type(failure).__name__}: '
                              f'{failure}')
        said = {**{k: v for k, v in made.items() if k != 'accessor'}, 'table': name, 'rowCount': rows,
                **({'pulledBy': receipt} if receipt is not None else {})}
        return Answer(200, 'application/json', json.dumps(said).encode('utf-8'))

    def _warehouse_snap(self, table: _WarehouseTable, asked: dict[str, Any]) -> Answer:
        """A warehouse grid's copy (C3): the pull planned with the table's live runtime, counted and pulled on the
        warehouse as the signed-in person, its Arrow chunks landed in the copy."""
        if asked['runtime'] != table.runtime:
            return _said(422, table.refused_runtime(asked['runtime']) or '')
        on = _on_planner_threads()
        planned = on(self.planner.plan, asked['pull'], table.held, table.runtime)
        try:
            plan = on(self.planner.read, planned)
        except LegendError as refused:
            return _said(422, f'the snap could not be planned: {refused}')
        try:
            # DataCube's own code: what says a count past the limit, and what models the copy
            headless = self._headless()
        except Exception as failure:
            return _said(422, f'{type(failure).__name__}: {failure}')
        try:
            # counted where the rows are, before anything is copied (the tab's preflight), by the catalog's own
            # database's statement (a Postgres catalog's, Postgres's: D5)
            (counting,) = _host.statements({'op': 'count', 'from': {'query': plan.sql}},
                                           database_type=table.database_type)
            counted = _arrow_of(table.warehouse.arrow_chunks(counting))
            rows = int(counted.column(0)[0].as_py()) if counted.num_rows else 0
            if rows > _MAX_SNAP_ROWS:
                return _snap_refused(headless, rows)
            receipt: dict[str, Any] = {}
            pulled = _arrow_of(table.warehouse.arrow_chunks(plan.sql, receipt.update))
        except warehouse.SessionExpired:
            # said as one (401: the page offers to sign in again), by the route
            raise
        except (warehouse.WarehouseError, pa.ArrowInvalid) as refused:
            return _said(422, f'the snap could not be pulled: {refused}')
        landing = '__legend_lite_snap'

        def land(connection: Any, target: dict[str, Any]) -> None:
            connection.register(landing, pulled)
            try:
                _host.run(connection, {'op': 'relation', 'kind': 'table', 'replace': True, 'target': target,
                                       'from': {'relation': _host.name(landing)}})
            finally:
                connection.unregister(landing)

        return self._landed(headless, land, rows, {
            'plane': 'warehouse', 'where': receipt.get('where', ''), 'as': receipt.get('as', ''),
            **({'statementId': receipt['statementId']} if 'statementId' in receipt else {}),
            **({'serverRows': receipt['serverRows']} if 'serverRows' in receipt else {})})

    def _served_snap(self, asked: dict[str, Any]) -> Answer:
        """A grid's copy over a model this engine serves (a frame's, a page's file's): planned on the compiler's thread,
        counted and copied by the frames' database, the frames held still -- a frame copied as it is now."""
        assert self._headless is not None
        try:
            # asked for before the frames are held (the session's lock first, then the frames')
            headless = self._headless()
        except Exception as failure:
            return _said(422, f'{type(failure).__name__}: {failure}')
        # its turn first, then the frames held: the copy is landed and modelled by DataCube's code inside (D15)
        with headless.turn(), self.frames.serving() as models:
            self._noted(models)
            held = self.frames.model_for(asked['model'])
            if held is None:
                return _said(404, 'this engine serves no model of that text: it snaps a page\'s frames and files, and '
                                  'warehouse tables it opened')
            planned = self.planner.plan(asked['pull'], held, asked['runtime'])
            try:
                plan = self.planner.read(planned)
            except LegendError as refused:
                return _said(422, f'the snap could not be planned: {refused}')
            (counting,) = _host.statements({'op': 'count', 'from': {'query': plan.sql}}, database_type='DuckDB')
            try:
                rows = int(self.frames.connection.execute(counting).fetchone()[0])
            except duckdb.Error as refused:
                return _said(422, f'the snap could not be counted: {refused}')
            if rows > _MAX_SNAP_ROWS:
                return _snap_refused(headless, rows)

            def land(connection: Any, target: dict[str, Any]) -> None:
                _host.run(connection, {'op': 'relation', 'kind': 'table', 'replace': True, 'target': target,
                                       'from': {'query': plan.sql}})

            return self._landed(headless, land, rows, None)

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
        compiler's threads; its SQL runs on the warehouse as the signed-in person, the server's Arrow chunks the rows,
        typed by the plan as Python's own are (a grid's copy is a table of the frames' database, planned over its own
        model: ``_plan_and_run``). The page sends Pure, never SQL (R4). An expired session is 401, its words
        the driver's, with ``expired`` naming the warehouse and the person (the page offers to sign in again)."""
        asked = json.loads(body)
        if not all(isinstance(asked.get(k), str) for k in ('lambda', 'model', 'runtime')):
            return _said(400, 'a plan-and-run is {"lambda", "model", "runtime"}, each a string')
        wrong = table.refused_runtime(asked['runtime'])
        if wrong is not None:
            return _planned({'plan': self.planner.refused_plan(wrong)})
        on = _on_planner_threads()
        planned = on(self.planner.plan, asked['lambda'], table.held, asked['runtime'])
        try:
            plan = on(self.planner.read, planned)
        except LegendError:
            return _planned({'plan': planned})
        try:
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

    def _plan_and_run(self, body: str, over: Reading | None = None) -> Answer:
        """PLANANDRUN on Python (§15.2): ``{"lambda", "model", "runtime", "project"?}`` -- the plan request the page's
        planner makes (lite-planner.ts ``planRequest``), its model as text -- planned against the model Python holds of
        that text by the planner function the tab's worker calls (``planJson``), its SQL run where its rows are, all in
        this one call: the page sends Pure, never SQL. A model's rows are the tables this engine serves (a page's frames
        and files, a snap's copies; a model that reads only them, R4) -- or, ``project`` (a saved query's: C5), its
        project's own Data rows, in a database of the project's own; never one read in the other's place (F28: a table
        not served is refused, never a project's sample rows read instead). The answer is the database's rows as Arrow,
        the planner's answer in their schema's metadata (``legend.plan``), which the page reads as its planner reads its
        own; or, no rows, JSON: the planner's refusal (``plan``), or its plan and why its SQL was refused (``error``).
        ``over``: the read of the frames it runs in (D15) -- a call of DataCube's made in one, or Python's own rows;
        none, a read of its own (a tab's call: the frames' lock held only to begin it, F35)."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a plan-and-run is {"lambda", "model", "runtime"}, as JSON')
        if not (isinstance(asked, dict) and all(isinstance(asked.get(k), str) for k in ('lambda', 'model', 'runtime'))):
            return _said(400, 'a plan-and-run is {"lambda", "model", "runtime"}, each a string')
        if asked.get('project') is True:
            return self._project_plan_and_run(asked, self.planner.refused_plan(_NO_PROJECT_ROWS))
        if over is None:
            with self.frames.reading() as read:
                return self._plan_and_run_over(asked, read)
        return self._plan_and_run_over(asked, over)

    def _plan_and_run_over(self, asked: dict[str, str], over: Reading) -> Answer:
        """A plan-and-run over a read of the frames (D15): planned by Python's planner against the model this engine
        serves of that text -- the tables as the read saw them, the frames' lock taken only while it plans -- and run on
        the read's own cursor, as the read sees the database. A model of a table not served now is refused, naming it
        (F28). The frames' models noted as last read, never older than the read (F30)."""
        self._noted(self.frames.models())
        planned = self.frames.plan_over(asked['model'], lambda held: self.planner.plan(asked['lambda'], held, asked['runtime']))
        if planned is None:
            return _planned({'plan': self.planner.refused_plan(self._unserved(asked['model']))})
        try:
            plan = self.planner.read(planned)
        except LegendError:
            return _planned({'plan': planned})
        try:
            rows = over.run(plan)
        except (duckdb.Error, UntypedColumn) as failure:
            return _planned({'plan': planned, 'error': f'{type(failure).__name__}: {failure}'}, 422)
        return _arrow_rows(rows, planned)

    def _unserved(self, model: str) -> str:
        """Why a model is not planned here, in words: the tables it reads that this engine does not serve now."""
        missing = self.frames.unserved(model)
        named = f': it reads {", ".join(missing)}, which it does not serve now' if missing else ''
        return ('this engine reads only the tables it serves -- a page\'s frames and files, a snap\'s copies, a model '
                f'that reads only them{named}')

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
        ``{"lambda", "model", "runtime", "project"?}``, as plan-and-run takes it, planned by the same planner function
        against the model Python holds of that text -- a page's frame or file, a warehouse table; ``project``, a saved
        query's project's own (C5) -- by plan-and-run's one rule of where a model's rows are (F28), and never run. JSON:
        ``{"plan"}``, the planner's answer, a refusal too, as plan-and-run carries it (``legend.plan``)."""
        try:
            asked = json.loads(body)
        except ValueError:
            return _said(400, 'a plan is {"lambda", "model", "runtime"}, as JSON')
        if not (isinstance(asked, dict) and all(isinstance(asked.get(k), str) for k in ('lambda', 'model', 'runtime'))):
            return _said(400, 'a plan is {"lambda", "model", "runtime"}, each a string')
        if asked.get('project') is True:
            with self.projects.using(asked['model']) as project:
                if project is None:
                    return _planned({'plan': self.planner.refused_plan(_NO_PROJECT_ROWS)})
                return _planned({'plan': self.planner.plan(asked['lambda'], project.held, asked['runtime'])})
        with self._warehouse_lock:
            table = self._warehouse_tables.get(asked['model'])
        if table is not None:
            # the runtime its run takes, refused as its run refuses it
            wrong = table.refused_runtime(asked['runtime'])
            if wrong is not None:
                return _planned({'plan': self.planner.refused_plan(wrong)})
            return _planned({'plan': self.planner.plan(asked['lambda'], table.held, asked['runtime'])})
        self._noted(self.frames.models())
        planned = self.frames.plan_over(asked['model'], lambda held: self.planner.plan(asked['lambda'], held, asked['runtime']))
        if planned is None:
            return _planned({'plan': self.planner.refused_plan(self._unserved(asked['model']))})
        return _planned({'plan': planned})

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
        model, runtime and source as they are now (a frame read again), and its title. Computed on the compiler's
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
        """A page's versions (``page``, its key): ``{"version": n, "frames": {name: n}}``, the page's own and each of its
        frames' (``page_versions``), at once -- the tab open on it saying who it is and the version it shows
        (``follower``, ``at``: ``_followed``) -- or 404 when the engine serves no such page (it was closed). A tab asks
        about once a second; nothing is held open while it waits, so pages in many tabs never use up the browser's six
        connections to one origin. (A frame is shown as a page of one grid: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
        P12.)"""
        asked = parse_qs(query)
        if 'page' not in asked:
            return _said(400, 'a version is asked of a page (page=<its key>)')
        versions = self.page_versions(asked['page'][0])
        if versions is None:
            return _said(404, f'this engine serves no page named {asked["page"][0]!r}')
        refused = self._followed(asked['page'][0], asked, asked.get('at', [''])[0])
        if refused is not None:
            return refused
        return Answer(200, 'application/json', json.dumps(versions).encode('utf-8'))

    def _page(self, query: str) -> Answer:
        """A page it serves (keeper.py: shown somewhere) as it is now: ``{"version": n, "page": <its document>, "held": [<every
        id it has held>]}``, or 404.
        Asked ``since=v`` by a page open at version ``v``: what moved it from there, ``{"version": n, "commands":
        [...]}``, when every version since was moved by commands (Python's or an open page's) -- else the page, as asked
        without."""
        asked = parse_qs(query)
        name = asked.get('page', [''])[0]
        since = asked.get('since', [''])[0]
        if since and not since.isdigit():
            return _said(400, 'a page asks what moved it since the version it opened (since=n)')
        kept = self.keeper.current(name) if self.keeper.served(name) else None
        if kept is None:
            return _said(404, f'this engine serves no page named {name!r}')
        refused = self._followed(name, asked, since)
        if refused is not None:
            return refused
        if since:
            version, commands = self.keeper.changes(name, int(since))
            if commands is not None:
                return Answer(200, 'application/json',
                              _json.dumps({'version': version, 'commands': commands}).encode('utf-8'))
            kept = self.keeper.current(name) or kept
        # every id it has held with it: an open page gives none of them out again (B12)
        return Answer(200, 'application/json', _json.dumps({'version': kept[0], 'page': kept[1],
                                                            'held': self.keeper.held(name)}).encode('utf-8'))

    def _page_said(self, query: str, body: str) -> Answer:
        """AN OPEN PAGE'S WRITE (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4: the host applies the commands): ``{"edits"}``,
        the edits it made at the version it is at, which the page's keeper has DataCube apply to the page as it is now
        and keeps what they make (keeper.py ``settle``) -- or ``{"restore"}``, the page whole (an undo, a redo: the plan's
        N2), read by DataCube's own reader (headless pageWritten: a page of no grid too) and kept only at the version it
        was made at (``restore``). Every frame the page then reads is one this engine serves. ``{"version", "combined",
        "renamed"}``: the page's version now, whether the keeper made another page than the writer's (its edits made again
        on top, or an id given out afresh: the page then opens what the keeper made), each id given out afresh. Refused
        -- an edit that does not apply there, a restore made against a version moved on from, a frame not served -- 409,
        and the page opens what the keeper has. Copies no page reads any more are let go."""
        asked = parse_qs(query)
        name = asked.get('page', [''])[0]
        version = asked.get('version', [''])[0]
        if not version.isdigit():
            return _said(400, 'a page says what it is with the version it opened (version=n)')
        if not self.keeper.served(name):
            return _said(404, f'this engine serves no page named {name!r}')
        if self._headless is None:
            return _said(404, 'this engine keeps no page: legend-lite reads one with DataCube\'s own code, headless, '
                              'which this engine was not given')
        refused = self._followed(name, asked, version)
        if refused is not None:
            return refused
        try:
            written = _json.loads(body)
        except ValueError:
            return _said(400, 'a page\'s write is JSON: {"edits"} or {"restore"}')
        if not isinstance(written, dict) or sorted(written) not in (['edits'], ['restore']) \
                or ('edits' in written and not isinstance(written['edits'], list)):
            return _said(400, 'a page\'s write is {"edits": [<its edits>]} -- or, an undo or a redo, {"restore": <the page>}')

        try:
            if 'restore' in written:
                # asked for holding nothing (the session's lock first, then the frames')
                said = _json.loads(self._headless().call('pageWritten', _json.dumps(written['restore'])))
                if 'problems' in said:
                    return _said(422, '; '.join(said['problems']))
                reads = said['reads']
                # the keeper's admission of a page by what it reads: the one rule Python's own writes are admitted by too
                refused = self.frames.unadmitted(reads)
                if refused is not None:
                    return _said(422, refused)
                settled = Settled(self.keeper.restore(name, int(version), _json.loads(said['page']), reads, reads['ids']),
                                  False, {})
            else:
                settled = self.keeper.settle(name, int(version), written['edits'], self.frames.unadmitted)
        except Refused as refused:
            return _said(409, str(refused))
        except Exception as failure:
            # a failure of the library's, said as one (as the API's are) -- never taken for a refusal or a page not served
            return _said(500, f'{type(failure).__name__}: {failure}')
        self.sweep()
        return Answer(200, 'application/json', json.dumps({'version': settled.version, 'combined': settled.combined,
                                                           'renamed': settled.renamed}).encode('utf-8'))

    def _followed(self, name: str, asked: Mapping[str, list[str]], at: str) -> Answer | None:
        """A PAGE OPEN ON ``name`` SAYS WHERE IT IS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17): ``follower``, the id it
        asks by, and ``at``, the version it shows (none while it opens or follows one: the version it said last stands)
        -- kept by the page's keeper, so the copies that version reads stay until it moves past them; the ones it moved
        past are let go. A request that names no follower says nothing. None, or the request refused."""
        follower = asked.get('follower', [''])[0]
        if not follower:
            return None if 'at' not in asked else _said(400, 'a page says the version it shows with who it is (follower=<its id>)')
        if not _FOLLOWER.fullmatch(follower) or (at and not at.isdigit()):
            return _said(400, 'a page open on another says who it is (follower=<letters, digits and -, at most 64>) and '
                              'the version it shows (a whole number)')
        if self.keeper.follow(name, follower, int(at) if at else None):
            self.sweep()
        return None

    def page_versions(self, name: str) -> dict[str, Any] | None:
        """A page's version, and each of its frames' (the frames its cubes read), or None when it serves no such page."""
        kept = self.keeper.current(name) if self.keeper.served(name) else None
        if kept is None:
            return None
        return {'version': kept[0], 'frames': {frame: self.version(frame) for frame in self.keeper.frames(name) or []}}

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
        served with -- a frame's columns changed -- is a change its page is told of."""
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
        # the API, an open page's write (page.json: the page keeper's), a page's file in or out, the
        # saved pages, the page's settings, PlanAndRun's calls, a warehouse's and a grid's Snap
        if not (url.path.startswith(_API) or url.path.startswith(_LITE_API) or _model_home(url.path) or url.path in (
                '/page.json', _FILE_IN, _FILE_PART, _FILE_OUT, _REMOTE, _SETTINGS, _PLAN_AND_RUN, _PLAN, _WAREHOUSE,
                _SNAP, _OPENED, _JOIN, _SOURCE)):
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
