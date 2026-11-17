"""LEGEND-ENGINE'S ROUTES ON PYTHON'S ENGINE, AT THE EDGE (the edge program's H5, 2026-11-17: moved from
``legend_lite.engine`` whole, once no product client asked them -- DataCube, Query, Studio and the query store ask
lite's own routes): legend-engine's ``pure/v1`` API (its grammar, compilation and generatePlan, answered by the native
library's edge entry ``lite_pure_v1``), its ``execution/execute`` in upstream's Arrow format and in its JSON (planned by
the edge's entries, the SQL run on the engine's own DuckDB by the run side, the answer written by legend-lite's server's
writer), and legend-engine's saved-query store at ``/api/pure/v1/query`` (its pages at ``.../dataCube``) in
legend-engine's words (the edge's native entries ``lite_saved_queries``, ``lite_saved_cubes``). The product answers
none of these: it asks this package's ``EngineRoutes`` through its seam (``_edge_seam``), and only when the edge's door
composed the engine."""

from __future__ import annotations

import contextlib
import json
import traceback
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa
import pyarrow.ipc

from .. import _json, compiler
from .._home import queries_folder
from .._library import library
from .._planned import from_plan
from .._pool import _workers
from .._raised import declare_sites
from .._reads import run_arrow, run_rows_json
from ..frames import redact_held

if TYPE_CHECKING:
    from ..engine import Answer, Engine

# legend-engine's API, as every Legend server mounts it
API = '/api/pure/v1/'
EXECUTE = API + 'execution/execute'
# legend-engine's query store, and DataCube's saved pages beside it (/api/pure/v1/query/dataCube): the saved queries'
# and pages' rules over this user's folders, in legend-engine's words
QUERIES = '/api/pure/v1/query'
CUBES = QUERIES + '/dataCube'


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
    return _http(library().call('lite_pure_v1', path, query, body))


def execute_plan(body: str, models: list[str]) -> EngineAnswer:
    """The plan half of execute in upstream's Arrow format: for an ``ExecuteInput`` over one of ``models`` (the only
    models this host runs), an answer whose body is ``{"sql", "metadata"}`` -- the SQL to run, and the Arrow schema
    metadata upstream's answer carries; any other answer is a refusal, to be sent as it is."""
    return _http(library().call('lite_execute_plan', body, _json.dumps(models)))


def execute_json_plan(body: str, models: list[str]) -> EngineAnswer:
    """The plan half of execute in legend-engine's JSON, for a host that runs the SQL on its own database: an answer
    whose body is ``{"sql", "run", "shape", "resultColumns", "builder", ...}`` -- ``run`` the typed statement, whose
    rows go back to :func:`execute_json_answer`; any other answer is a refusal, to be sent as it is."""
    return _http(library().call('lite_execute_json_plan', body, _json.dumps(models)))


def execute_json_answer(plan: str, rows: str) -> EngineAnswer:
    """Execute's answer in legend-engine's JSON: ``plan`` the plan half's body, ``rows`` the rows the database answered
    its ``run`` statement with (a JSON array of objects, each a column's name and its cell as the driver read it),
    decoded by the plan's columns and written by legend-lite's server's own writer -- the server's answer text."""
    return _http(library().call('lite_execute_json_answer', plan, rows))


def refusal(message: str) -> EngineAnswer:
    """A database's refusal, as legend-lite's server answers one (``500``, legend-engine's error shape: the edge's
    ``EngineEnvelope.refused``)."""
    return _http(library().call('lite_pure_v1_refusal', message))


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


class EngineRoutes:
    """LEGEND-ENGINE'S ROUTES, served by an engine composed at the edge's door (``legend_lite.edge.engine``): POSTs of
    ``/api/pure/v1/...`` (execute among them), and the query store at ``/api/pure/v1/query`` (GET, POST, PUT, DELETE);
    anything else is no call of theirs."""

    def serves(self, method: str, path: str) -> bool:
        if _under(path, QUERIES):
            return True
        return method == 'POST' and path.startswith(API)

    def answer(self, engine: Engine, method: str, path: str, query: str, body: str | None) -> Answer:
        from ..engine import _said, _stored
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
