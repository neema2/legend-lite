"""LEGEND-ENGINE'S ROUTES ON PYTHON'S ENGINE, AT THE EDGE (the edge program's H5, 2026-11-17: moved from
``legend_lite.engine`` whole, once no product client asked them -- DataCube, Query, Studio and the query store ask
lite's own routes): legend-engine's ``pure/v1`` API (its grammar, compilation and generatePlan, answered by the native
library's edge entry ``lite_pure_v1``), its ``execution/execute`` in upstream's Arrow format and in its JSON (planned by
the edge's entries, the SQL run on the engine's own DuckDB by the run side, the answer written by legend-lite's server's
writer), and legend-engine's saved-query store at ``/api/pure/v1/query`` (its pages at ``.../dataCube``) in
legend-engine's words (the edge's native entries ``lite_saved_queries``, ``lite_saved_cubes``). Every one of those
entries is the ENGINE library's (//native:engine, K2: the user's "product root, edge opt-in", 2026-11-18), asked
through the edge's key (``_engine``); the product library exports none. The product answers
none of these: it asks this package's ``EngineRoutes`` through its seam (``_edge_seam``), and only when the edge's door
composed the engine."""

from __future__ import annotations

import contextlib
import json
import traceback
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa
import pyarrow.ipc

from .. import _json, compiler
from .._home import THIS_USER, locked, queries_folder
from .._planned import from_plan
from .._pool import _on_compiler_threads, _workers
from .._raised import declare_sites
from .._reads import run_arrow, run_rows_json
from ..frames import redact_held
from ._engine import engine_library

if TYPE_CHECKING:
    from ..engine import Answer, Engine

# legend-engine's API, as every Legend server mounts it
API = '/api/pure/v1/'
EXECUTE = API + 'execution/execute'
# legend-engine's query store, and DataCube's saved pages beside it (/api/pure/v1/query/dataCube): the saved queries'
# and pages' rules over this user's folders, in legend-engine's words
QUERIES = '/api/pure/v1/query'
CUBES = QUERIES + '/dataCube'
# legend-engine's current user (N4 of PR #118's fix round, 2026-11-18: the K2 move's last route, the product's engine
# serving it until then): the store's user, whom its saved queries are kept for -- lite's own is POST /api/lite/v1/user
CURRENT_USER = '/api/server/v1/currentUser'


def _under(path: str, mount: str) -> bool:
    return path == mount or path.startswith(mount + '/')


@dataclass(frozen=True)
class EngineAnswer:
    """An HTTP answer of legend-engine's ``pure/v1`` API, as legend-lite's server gives it: its status, media type and
    body (the edge's native entries: ``OK`` and ``<status>\\n<type>\\n<body>``, or lite's error)."""

    status: int
    content_type: str
    body: str


def _http(text: str) -> EngineAnswer:
    # the native library's fold, read as every binding reads it (compiler: OK and the answer, or ERR and lite's error)
    status, content_type, body = compiler.folded(text).split('\n', 2)
    return EngineAnswer(int(status), content_type, body)


def pure_v1(path: str, query: str, body: str) -> EngineAnswer:
    """One call of legend-engine's ``pure/v1`` API, answered by legend-lite's server code (the edge's ``PureV1Routes``,
    ``lite_pure_v1``) -- every endpoint it serves but execute's run: ``path`` as requested (``/api/pure/v1/...``),
    ``query`` its raw query string (``''`` for none)."""
    return _http(engine_library().call('lite_pure_v1', path, query, body))


def execute_plan(body: str, models: list[str]) -> EngineAnswer:
    """The plan half of execute in upstream's Arrow format: for an ``ExecuteInput`` over one of ``models`` (the only
    models this host runs), an answer whose body is ``{"sql", "metadata"}`` -- the SQL to run, and the Arrow schema
    metadata upstream's answer carries; any other answer is a refusal, to be sent as it is."""
    return _http(engine_library().call('lite_execute_plan', body, _json.dumps(models)))


def execute_json_plan(body: str, models: list[str]) -> EngineAnswer:
    """The plan half of execute in legend-engine's JSON, for a host that runs the SQL on its own database: an answer
    whose body is ``{"sql", "run", "shape", "resultColumns", "builder", ...}`` -- ``run`` the typed statement, whose
    rows go back to :func:`execute_json_answer`; any other answer is a refusal, to be sent as it is."""
    return _http(engine_library().call('lite_execute_json_plan', body, _json.dumps(models)))


def execute_json_answer(plan: str, rows: str) -> EngineAnswer:
    """Execute's answer in legend-engine's JSON: ``plan`` the plan half's body, ``rows`` the rows the database answered
    its ``run`` statement with (a JSON array of objects, each a column's name and its cell as the driver read it),
    decoded by the plan's columns and written by legend-lite's server's own writer -- the server's answer text."""
    return _http(engine_library().call('lite_execute_json_answer', plan, rows))


def refusal(message: str) -> EngineAnswer:
    """A database's refusal, as legend-lite's server answers one (``500``, legend-engine's error shape: the edge's
    ``EngineEnvelope.refused``)."""
    return _http(engine_library().call('lite_pure_v1_refusal', message))


def _stored(entry: str, folder: Path, *asked: str) -> Answer:
    """One call of legend-engine's store at its mount (``lite_saved_queries``, ``lite_saved_cubes``: legend-lite's own
    rules in legend-engine's words, the engine library's), as this user, under the lock every Python process takes on
    the folder, the call on one of the compiler's threads: its answer as the store gave it, a failure of the library's
    said (500) -- as the product's store at lite's mount is answered (engine._stored, _home.store_call)."""
    from .._home import THIS_USER, reowned_once
    from ..engine import Answer, _said
    from .._folded import unfold_status
    try:
        with locked(folder):
            reowned_once(folder)
            said = _on_compiler_threads(lambda: engine_library().call(entry, str(folder), *asked, THIS_USER))
        # read by the product's one reader (H4's P3, 2026-11-19: until then the layout retired on 2026-11-17, so a
        # refusal of the library's was said as its raw JSON text); a refusal in its place said below in its own words
        status, answered = unfold_status(said)
    except Exception as failure:
        traceback.print_exc()
        return _said(500, f'{type(failure).__name__}: {failure}')
    if int(status) == 204:
        return Answer(204, 'text/plain; charset=utf-8', b'')
    return Answer(int(status), 'application/json', answered.encode('utf-8'))


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


# the edge's door's own key: legend-engine's routes are made by the door alone (``legend_lite.edge.engine``)
_DOOR = object()


class EngineRoutes:
    """LEGEND-ENGINE'S ROUTES, served by an engine composed at the edge's door (``legend_lite.edge.engine``): POSTs of
    ``/api/pure/v1/...`` (execute among them), the query store at ``/api/pure/v1/query`` (GET, POST, PUT, DELETE), and
    ``/api/server/v1/currentUser`` (GET, any other method its 405); anything else is no call of theirs. Made by the
    door alone, ONE DOOR EACH (the PR #118 follow-up, 2026-11-19, R11(b), HOSTS-7: as Java's edge composes its server
    in one place): a caller
    composing an engine of its own with them is refused by name. The product cannot name this class (the layer rule),
    so the door's key is checked here, where the routes are made. Reusing the routes the door made on a second engine
    (``Engine(frames, edge=edge.engine(f).edge)``) is allowed: they were composed at the door, the engine library
    checked there (F8 of PR #121's review, 2026-11-19)."""

    # F8 of PR #121's review (2026-11-19): the door's key alone was a marker -- a subclass skipping __init__, or an
    # instance made by object.__new__, served the routes without it. Now no class extends this one, every route is
    # served only by an instance the door made, and no module of the package but the door names the key or makes the
    # routes (test_edge.TheEdgesDoor's source rule, over the package as shipped).

    def __init_subclass__(cls, **kwargs: Any) -> None:
        raise TypeError("legend-engine's routes are composed by the edge's door alone: EngineRoutes takes no subclass"
                        " (an engine that serves them is legend_lite.edge.engine(frames))")

    def __init__(self, door: object = None) -> None:
        if door is not _DOOR:
            raise TypeError("legend-engine's routes are composed by the edge's door alone: an engine that serves them is"
                            " legend_lite.edge.engine(frames)")
        self._door = door

    def _by_door(self) -> None:
        """Refused unless the door made this instance (an object.__new__ of the class never ran its __init__)."""
        if self.__dict__.get('_door') is not _DOOR:
            raise TypeError("legend-engine's routes are composed by the edge's door alone: these were not made by it"
                            " (an engine that serves them is legend_lite.edge.engine(frames))")

    def serves(self, method: str, path: str) -> bool:
        self._by_door()
        if _under(path, QUERIES):
            return True
        if path == CURRENT_USER:
            return True
        return method == 'POST' and path.startswith(API)

    def answer(self, engine: Engine, method: str, path: str, query: str, body: str | None) -> Answer:
        self._by_door()
        from ..engine import Answer, _said
        if path == CURRENT_USER:
            # who a page's caller is: "anonymous", whom the stores keep queries for (there is no sign-in) -- GET only,
            # as upstream serves it (a JAX-RS @GET; its shipped AnonymousClient's profile is "anonymous"), any other method
            # 405 in the body Java's edge answers (EdgeServer.currentUser; register S377): the user's ruling of
            # 2026-11-19 ~11:27 UTC, "1 yes, 2 a1" -- until then the GET answered 'this user' and any other method fell
            # through to the product's 404
            if method != 'GET':
                return Answer(405, 'application/json', b'{"error":"Method not allowed"}')
            return Answer(200, 'application/json', json.dumps(THIS_USER).encode('utf-8'))
        if _under(path, QUERIES):
            # on the caller's thread, as the product's store: it waits for the folder's lock holding no compiler thread
            if method not in ('GET', 'POST', 'PUT', 'DELETE'):
                return _said(405, 'the query store answers GET, POST, PUT and DELETE')
            if _under(path, CUBES):
                return _stored('lite_saved_cubes', engine.pages_folder(), method, path[len(CUBES):], query, body or '')
            return _stored('lite_saved_queries', queries_folder(), method, path[len(QUERIES):], query, body or '')
        if method == 'POST' and path.startswith(API):
            return _workers().submit(self._api, engine, path, query, body or '').result()
        from .._edge_seam import NO_EDGE_ROUTES
        return NO_EDGE_ROUTES.answer(engine, method, path, query, body)

    def _api(self, engine: Engine, path: str, query: str, body: str) -> Answer:
        """An API call's answer, computed on the compiler's pool."""
        from ..engine import Answer, _said
        try:
            if path == EXECUTE:
                return self._execute(engine, body) if _arrow_requested(query) else self._execute_json(engine, body)
            answer = pure_v1(path, query, body)
        except Exception as failure:
            # never a dropped call: the database refusing is an answer; anything else is a bug, logged whole, and
            # answered the same way (as legend-lite's server answers both)
            if not isinstance(failure, duckdb.Error):
                traceback.print_exc()
            # no value this engine holds in its words (a presigned link's URL; found with the C5 follow-up's item 1)
            message = redact_held(f'{type(failure).__name__}: {failure}', engine.frames.held_values())
            try:
                answer = refusal(message)
            except Exception:
                # the compiler itself failing: still an answer, its own words
                traceback.print_exc()
                return _said(500, message)
        return Answer(answer.status, answer.content_type, answer.body.encode('utf-8'))

    def _execute(self, engine: Engine, body: str) -> Answer:
        """execute in upstream's Arrow format: the compiler plans over the frames' models, DuckDB runs its SQL, and the
        rows go back as upstream writes them. One query at a time (the frames are held still while it runs); the
        answer is compressed outside."""
        from ..engine import Answer
        with engine.frames.serving() as models:
            engine._noted(models)
            with self._project_of(engine, body, models) as (served, connection):
                answer = execute_plan(body, served)
                if answer.status != 200:
                    return Answer(answer.status, answer.content_type, answer.body.encode('utf-8'))
                planned = json.loads(answer.body)
                # the raises its statement can send, worded by the planner (_raised.declare_sites; option (D))
                declare_sites(planned, 'an execute plan')
                # its values (a data: payload: compiler.plan_params, M3) bound to its `?`s; a failure's words name no
                # value this engine holds (a bearer URL: M2) -- the plan read here, then handed to its run
                sql, params = from_plan(planned['sql']), compiler.plan_params(planned.get('plan'))
                rows = run_arrow(connection, sql, engine.frames.held_values(), params)
        # upstream's headers for this answer (measured, legend-engine 4.145.0)
        return Answer(200, 'application/json', _arrow_ipc(rows, planned['metadata']),
                      {'x-legend-response-format': 'FormatNotSet'})

    def _execute_json(self, engine: Engine, body: str) -> Answer:
        """execute in legend-engine's JSON: the compiler plans over the frames' models, DuckDB runs the typed statement,
        and its rows, handed back as the driver read them, are decoded by the plan's columns and written by the server's
        own writer -- the server's answer text, as the tab's is. One query at a time (the frames are held still while
        it runs)."""
        from ..engine import Answer
        with engine.frames.serving() as models:
            engine._noted(models)
            with self._project_of(engine, body, models) as (served, connection):
                planned = execute_json_plan(body, served)
                if planned.status != 200:
                    return Answer(planned.status, planned.content_type, planned.body.encode('utf-8'))
                said = json.loads(planned.body)
                declare_sites(said, 'an execute plan')
                # the plan read here, then handed to its run, which hands its rows back as the driver read them
                sql, params = from_plan(said['run']), compiler.plan_params(said.get('plan'))
                rows = run_rows_json(connection, sql, engine.frames.held_values(), params)
        answer = execute_json_answer(planned.body, rows)
        return Answer(answer.status, answer.content_type, answer.body.encode('utf-8'))

    @contextlib.contextmanager
    def _project_of(self, engine: Engine, body: str, models: dict[str, str]) -> Iterator[tuple[list[str], Any]]:
        """Where an execute runs: over the frames' models, on the frames' connection -- or, for a project's model with
        Data rows (Query's or Studio's), over that model alone, on a cursor of its project's own database (C5). A model
        neither is stays refused by the compiler (a frame's that changed, say)."""
        served = list(models.values())
        model = _request_model(body)
        if model is None or model in served:
            yield served, engine.frames.connection
            return
        with engine.projects.using(model) as project:
            if project is None:
                yield served, engine.frames.connection
            else:
                yield [model], project.cursor
