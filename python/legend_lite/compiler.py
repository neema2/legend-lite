"""legend-lite's compiler, in Python: the same compiler the browser runs as WebAssembly, built native.

It works on PROTOCOL TREES (upstream's V1 lambda JSON, as Python dicts), the way DataCube builds its
queries: Pure text is one way to make a tree (``parse``), and a tree prints back as Pure
(``print_tree``). Typing and planning take a model's text, compiled for the call; a ``HeldModel`` is a model
compiled once and planned against many times, for as long as it is open.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import _json
from ._library import library
from ._planned import PlannedSql, PlanText, all_from_plan, from_plan, plan_from_answer
from ._raised import declare_sites
# the plan's records and its own data's readers, theirs since C5-P (2026-11-15): named here as they always were
from ._plans import (Column, Plan, PlanParamsRefused, host_rows, plan_binding, plan_params,  # noqa: F401
                     statement_params)

Tree = dict[str, Any]


class LegendError(Exception):
    """The compiler refused, in lite's one error shape (the lite API spec's §3, 2026-11-17): its ``kind`` -- LITE'S OWN,
    as the native library folds it (``LiteV1Api.RefusalKind``; never a constant of Python's: ``refusal_kinds``) --
    its ``message`` in lite's words, and, where there is one, its ``where`` (a position or a site) and, for a run's
    raise, the raise's own kind (``raise_``)."""

    def __init__(self, message: str, kind: str, where: str | None = None, raise_: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.kind = kind
        self.where = where
        self.raise_ = raise_


_KINDS: dict[str, str] | None = None


def refusal_kinds() -> dict[str, str]:
    """LITE'S ERROR KINDS, by name, as the product names them (``lite_refusal_kinds``: the one Java type of §3's kinds,
    each name and its wire text), read once: a refusal's ``kind`` is compared with these, never with a spelling of
    Python's own, so a rename in the product needs no edit here (the lite API spec's §3, 2026-11-17)."""
    global _KINDS
    if _KINDS is None:
        tag, _, rest = library().call('lite_refusal_kinds').partition('\n')
        if tag != 'OK':
            raise OSError(f"the native library did not say lite's error kinds: {rest[:200]}")
        _KINDS = _json.loads(rest)
    return _KINDS


def error_kind(name: str) -> str:
    """The wire text of the product's kind ``name``; a name the product does not say, refused by name (the
    orchestrator's ruling, 2026-11-17: never a raw lookup error)."""
    kinds = refusal_kinds()
    if name not in kinds:
        raise LegendError(f'an error kind this binding does not know: {name}', kinds.get('INTERNAL', 'internal'))
    return kinds[name]


def parameters_refused(message: str) -> LegendError:
    """A run's parameter values refused (the run side's ``_params.check``, its words ``message``) as lite's error: the
    product's ``parameters`` kind (the lite API spec's §3, ruled 6)."""
    return LegendError(message, error_kind('PARAMETERS'))


def refuses_the_text(refused: LegendError) -> bool:
    """Whether ``refused`` is the compiler's refusal of the TEXT it was given -- it does not parse, or does not compile
    -- rather than a failure of its own: the product's PARSE and COMPILE kinds (``refusal_kinds``)."""
    return refused.kind in (error_kind('PARSE'), error_kind('COMPILE'))


@dataclass(frozen=True)
class Answer:
    """An HTTP answer of legend-engine's ``pure/v1`` API, as legend-lite's server gives it: its status, media
    type and body."""

    status: int
    content_type: str
    body: str


def _answer(text: str) -> str:
    """The compiler's answer, or its refusal raised in lite's one error shape (``planner.Folded``: ``OK`` and the
    answer, or ``ERR`` and §3's ``{kind, message, where?, raise?}``). A kind the product does not say is refused by
    name (``_kind``)."""
    tag, _, rest = text.partition('\n')
    if tag == 'OK':
        return rest
    if tag == 'ERR':
        try:
            said = _json.loads(rest)
        except ValueError:
            said = None
        if not (isinstance(said, dict) and isinstance(said.get('kind'), str) and isinstance(said.get('message'), str)):
            raise OSError(f"the native library refused in no error shape lite's (§3): {rest[:200]!r}")
        if said['kind'] not in refusal_kinds().values():
            raise LegendError(f"an error kind this binding does not know: {said['kind']}", error_kind('INTERNAL'))
        raise LegendError(said['message'], said['kind'], said.get('where'), said.get('raise'))
    raise OSError(f'the native library answered something unrecognised: {text[:120]!r}')


def _http(text: str) -> Answer:
    status, content_type, body = _answer(text).split('\n', 2)
    return Answer(int(status), content_type, body)


def operation(op: str, request: dict[str, Any]) -> Any:
    """LITE'S OWN API, v1 (docs/records/lite-api-v1-2026-11-17.md §2, §5): one operation by its name (``query/parse``,
    ``model/check``, ...), its request in and its answer out, through the native entry the spec's one rule names
    (operation ``a/b`` -> ``lite_a_b``) -- the binding every public function below is a thin layer over. No
    ``query/run``: Python runs a plan on its own database (``Frames.run``)."""
    if op == 'user':
        return user(request)
    entry = OPERATIONS.get(op)
    if entry is None:
        raise ValueError(f"lite's API has no operation '{op}' this binding asks (it asks {sorted(OPERATIONS)})")
    return _json.loads(_answer(library().call(entry, _json.dumps(request))))


# §2's operations Python asks, each its native entry (the spec's one rule; query/run is the host's runner)
OPERATIONS = {
    'model/check': 'lite_model_check', 'model/elements': 'lite_model_elements', 'query/parse': 'lite_query_parse',
    'query/print': 'lite_query_print', 'query/type': 'lite_query_type', 'query/plan': 'lite_query_plan',
    'model/createClass': 'lite_model_create_class', 'model/createAssociation': 'lite_model_create_association',
    'model/createDataSpace': 'lite_model_create_data_space', 'ts/item': 'lite_ts_item', 'ts/statement': 'lite_ts_statement',
}


def user(request: dict[str, Any] | None = None) -> dict[str, str]:
    """``user`` (the spec's §2, a host's own operation): who the store keeps saved queries for -- this user, as Python's
    stores name the caller (_home.THIS_USER; there is no sign-in), ``{"user": <name>}``. Its request holds no key: one
    that does is refused by name (kind ``request``), as the server's route refuses it."""
    from ._home import THIS_USER
    if request:
        raise LegendError(f'a user request holds no key (it holds {", ".join(sorted(request))})', error_kind('REQUEST'))
    return {'user': THIS_USER}


def lite_route(path: str, body: str) -> tuple[int, str]:
    """LITE'S OWN API, v1, AS A ROUTE (the spec's §2, §3, §5; the host train, 2026-11-17): ``/api/lite/v1/<operation>``
    with its request JSON, answered by the operation's native entry (``operation``) -- 200 and the answer's JSON, or
    §3's error ``{kind, message, where?, raise?}`` with its kind's status: 422 for a run's raise (``data``), 500 for
    ``internal``, 400 for every other (ruled 1, 3, 6). Any other path is refused in lite's shape: the old route names
    went in one step (ruled 5). The one rule Python's server and DataCube's code in this process (_headless) answer by."""
    prefix = '/api/lite/v1/'
    op = path[len(prefix):] if path.startswith(prefix) else None
    if op not in OPERATIONS and op != 'user':
        return refused_route(LegendError(f'no such legend-lite API: {path}', error_kind('REQUEST')))
    try:
        # the user operation's request may be empty, as the server's route reads it
        asked = {} if op == 'user' and not body.strip() else _json.loads(body)
    except ValueError:
        asked = None
    if not isinstance(asked, dict):
        return refused_route(LegendError(f'{op}: the request is no JSON object', error_kind('REQUEST')))
    try:
        return 200, _json.dumps(operation(op, asked))
    except LegendError as refused:
        return refused_route(refused)


def refused_route(refused: LegendError) -> tuple[int, str]:
    """A refusal as lite's route answers it (the spec's §3): ``{kind, message, where?, raise?}`` and its kind's status
    -- 422 a run's raise (``data``), 500 ``internal``, 400 every other."""
    said: dict[str, Any] = {'kind': refused.kind, 'message': refused.message}
    if refused.where is not None:
        said['where'] = refused.where
    if refused.raise_ is not None:
        said['raise'] = refused.raise_
    status = 422 if refused.kind == error_kind('DATA') else 500 if refused.kind == error_kind('INTERNAL') else 400
    return status, _json.dumps(said)


def parse(text: str) -> Tree:
    """Pure text as its query's tree, e.g. ``parse("|1 + 1")`` (``query/parse``)."""
    return operation('query/parse', {'text': text})['query']


def print_tree(tree: Tree, style: str = 'pretty') -> str:
    """A query's tree as Pure text (``query/print``): ``pretty`` across lines (the default), or ``standard`` on one;
    any other style refused by name."""
    return operation('query/print', {'query': tree, 'style': style})['text']


def relation_type(model: str, tree: Tree) -> tuple[Column, ...]:
    """The columns a relation query returns, each its name and Pure type, compile-only (``query/type``)."""
    return tuple(Column(c['name'], c['type'], c) for c in operation('query/type', {'model': model, 'query': tree})['columns'])


def check(model: str) -> list[dict[str, Any]]:
    """A model's every error, each its kind and message, empty when it compiles (``model/check``)."""
    return operation('model/check', {'model': model})['errors']


def create_class(request: dict[str, Any]) -> dict[str, Any]:
    """DataCube's model edit making a class: the files to write, in the SDLC's change shape (``model/createClass``)."""
    return operation('model/createClass', request)


def create_association(request: dict[str, Any]) -> dict[str, Any]:
    """DataCube's model edit making an association (``model/createAssociation``)."""
    return operation('model/createAssociation', request)


def create_data_space(request: dict[str, Any]) -> dict[str, Any]:
    """DataCube's model edit making a data space (``model/createDataSpace``)."""
    return operation('model/createDataSpace', request)


def ts_statement(model: str, tree: Tree) -> Any:
    """A #TS query as its one statement (``ts/statement``)."""
    return operation('ts/statement', {'model': model, 'query': tree})


def ts_item(text: str) -> Any:
    """A #TS item's text, read (``ts/item``)."""
    return operation('ts/item', {'text': text})


def read_planned(answer: str) -> Plan:
    """``query/plan``'s tagged answer (``OK`` and ``{"plan": <lite's plan>}``, or ``ERR`` and §3's error) as a Plan, its
    refusal raised (``LegendError``): what the held planner answers (``lite_query_plan_held``) and DataCube's page
    reads (gridPlan)."""
    return read_plan_data(_json.loads(_answer(answer))['plan'])


def plan(model: str, tree: Tree, runtime: str) -> Plan:
    """A relation query planned against ``model`` through ``runtime`` (``query/plan``): lite's plan, which is data -- its
    SQL, its parameters' slots and its columns (``read_plan_data``)."""
    return read_plan_data(operation('query/plan', {'model': model, 'query': tree, 'runtime': runtime})['plan'])


def read_plan_data(plan: dict[str, Any]) -> Plan:
    """``query/plan``'s plan (PlanJson) as a Plan: its typed result's statement, its columns each its name and Pure
    type, its payloads, declared parameters and slots (``plan_binding``), the raises its statement can send declared
    (``_raised.declare_sites``). A plan whose root is not one statement's typed result is refused by name."""
    root = plan.get('root') if isinstance(plan, dict) else None
    statement = root.get('sql') if isinstance(root, dict) else None
    if not (isinstance(statement, dict) and isinstance(root.get('columns'), list)):
        raise PlanParamsRefused(f"a query's plan runs here as one statement's typed result, not a "
                                f"{root.get('_type') if isinstance(root, dict) else root!r}")
    declare_sites(plan, 'a query plan')
    payloads, parameters, slots = plan_binding(plan)
    columns = tuple(Column(c['name'], c['type'], c) for c in root['columns'])
    return Plan(from_plan(statement['statement']), columns, payloads, parameters, slots)


def plan_text(model: str, text: str, runtime: str) -> Plan:
    """The same, from Pure text: ``query/parse`` then ``query/plan`` (the lite API spec's §7)."""
    return plan(model, parse(text), runtime)


class HeldModel:
    """A model the compiler holds: compiled once, when it is made, and planned and typed against as often as asked,
    each time paying for the query alone (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §9). The same text held twice is
    the one model, compiled once. Held until ``close()`` (or the end of a ``with`` block); a model that does not compile
    is refused when it is made, and nothing is held. Safe from several threads: one plans against it at a time.
    ``over``: a library the model is compiled over (DataCube's Pure library), held by the compiler as a layer of its own,
    compiled once for every model held over it (docs/TS_V1_PROGRAM_2026_11_01.md A5); the answer is the one text's."""

    def __init__(self, model: str, over: str | None = None) -> None:
        self.model = model
        self.over = over
        self._handle: str | None = _answer(library().call('lite_hold_model', model) if over is None
                                           else library().call('lite_hold_model_over', model, over))

    def _held(self) -> str:
        if self._handle is None:
            raise ValueError('this model was let go (closed): hold it again')
        return self._handle

    @property
    def handle(self) -> str:
        """The library's handle of the model held: what a planner request of a held kind names (legend_lite._planner)."""
        return self._held()

    def plan(self, tree: Tree, runtime: str) -> Plan:
        """A relation query planned against the model held, through ``runtime`` (``query/plan`` over the binding's cache,
        the spec's §7a): lite's plan read as a Plan."""
        request = _json.dumps({'query': tree, 'runtime': runtime})
        return read_planned(library().call('lite_query_plan_held', self._held(), request))

    def relation_type(self, tree: Tree) -> tuple[Column, ...]:
        """The columns a relation query returns, each its name and Pure type, typed against the model held, compile-only
        (``query/type`` over the binding's cache)."""
        said = _json.loads(_answer(library().call('lite_query_type_held', self._held(), _json.dumps({'query': tree}))))
        return tuple(Column(c['name'], c['type'], c) for c in said['columns'])

    def close(self) -> None:
        """Lets the model go; closing it again does nothing."""
        if self._handle is not None:
            handle, self._handle = self._handle, None
            _answer(library().call('lite_release_model', handle))

    def __enter__(self) -> HeldModel:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def model_elements(text: str) -> list[dict[str, Any]]:
    """A model's text as its protocol elements (PureModelContextData's), as the compiler reads them (``model/elements``)."""
    return operation('model/elements', {'model': text})['elements']


def database_from_catalog(catalog: dict[str, Any]) -> dict[str, Any]:
    """A Pure Database for one table, written from its catalog rows, as DataCube writes one for a file.

    Takes ``{"path", "schema"?, "table", "convertible", "databaseType", "columns": [{"name", "dataType",
    "logicalType", ...}]}``, where ``databaseType`` is the database whose catalog it is (e.g. ``"DuckDB"``).
    Returns ``{"text", "source", "conversions", "excluded"}``: ``text`` is the Database's Pure text."""
    return _json.loads(_answer(library().call('lite_database_from_catalog', _json.dumps(catalog))))


def table_model(table: dict[str, Any]) -> dict[str, Any]:
    """The model for a table, from its catalog rows: the Database, its connection and runtime -- the one
    writer DataCube's tables and Python's frames share (the compiler's ``tableModelOrError``).

    Takes ``{"table", "schema"?, "pkg"? (default "local"), "convertible", "databaseType", "columns": [catalog
    rows]}``. Returns ``{"model", "runtime", "source", "accessor", "conversions", "copySelectList"?, "excluded"}``:
    ``model`` is Pure text, ``source`` the relation that reads the table (a tree; ``accessor`` its
    text), ``copySelectList`` the select list the table applies, rewritten in place so it holds the declared types,
    written by its own database (``convertible``; absent when the table is never rewritten).

    Given ``{"databaseType", "connection"? (default "host"), "tables": [{"table", "schema"?, "pkg"? (default
    "local"), "convertible", "columns"}]}`` instead, the model of SEVERAL tables of one database (a join's inputs, read
    in one place): each package's Database declaring its tables as each one's own model does, one connection, and a
    runtime binding every Database to it. Returns ``{"model", "runtime", "tables": [{"source", "accessor",
    "conversions", "excluded"}]}``, the tables in the order given; a table given twice is refused.
"""
    return _json.loads(_answer(library().call('lite_table_model', _json.dumps(table))))


def catalog_columns_sql(schema: str, table: str) -> PlannedSql:
    """The catalog question for one table of a DuckDB: SQL whose rows (``column_name``, ``data_type``,
    ``logical_type``, ``numeric_precision``, ``numeric_scale``, ``not_null``) describe its columns."""
    return from_plan(_answer(library().call('lite_catalog_columns_sql', schema, table)))


def session_setup(database_type: str) -> list[PlannedSql]:
    """What a session of the given database runs before it is queried, so that it answers as the planner's
    SQL expects: the dialect's own setup (a DuckDB session in UTC)."""
    return all_from_plan(_json.loads(_answer(library().call('lite_session_setup', database_type))))


def host_operations(database_type: str, operations: list[dict[str, Any]]) -> list[list[PlannedSql]]:
    """A host's named operations on its own database of the given type (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5):
    each operation's statements, in order -- what Python's host runs instead of spelling SQL (engine-client's
    host-operations.ts writes the same operations). One the database has not is refused by name, and so is one carrying
    SQL -- a source ``query``, a ``selectList``, a ``replacing`` expression -- by the platform (the final audit's item 1,
    2026-11-12: a host operation takes no SQL; a copy's conversions are the platform's, from its columns' catalog,
    ``convert``)."""
    answered = _json.loads(_answer(library().call('lite_host_operations', database_type, _json.dumps(operations))))
    return [all_from_plan(statements) for statements in answered]


def snap_stream(request: dict[str, Any]) -> PlanText:
    """A SNAPSHOT FROM A REMOTE PLANE, as a plan (the native library's Boundary.snapStream; the user's ruling of
    2026-11-11, "This is the right plan"): ``request`` is ``{"databaseType", "plane", "model", "runtime", "lambda",
    "schema"?, "table"}`` -- the name this host holds the remote plane under, the PULL (the model's text, its runtime and
    the query as a lambda's protocol JSON, which the planner plans in this call for the plane's own database: the user's
    ruling of 2026-11-12, "that is a real off platform problem" -- a request carries no statement), and the table of this
    database it lands in; never a row, never SQL (any other key is refused by name). The plan: one HOST_SESSION load
    whose stream step ``projects.run_host_load`` runs in one transaction."""
    # the plan as the planner answered it (the re-audit's M1): what run_host_load takes, and nothing else does
    return plan_from_answer(_answer(library().call('lite_snap_stream', _json.dumps(request))))


def snap_copy(request: dict[str, Any]) -> PlanText:
    """A SNAPSHOT'S LOCAL COPY, as a plan (the native library's Boundary.snapCopy; the final audit's item 1(b),
    2026-11-12): ``request`` is ``{"databaseType", "model", "runtime", "lambda", "schema"?, "table"}`` -- the PULL, which
    the planner plans in this call, and the table it is copied into; never SQL (any other key is refused by name). The
    plan: one write -- the table there before dropped, the table made from the pull, its data: payloads bound by digest
    -- in its target's transaction, which ``projects.run_write`` runs."""
    return plan_from_answer(_answer(library().call('lite_snap_copy', _json.dumps(request))))


def frame_load(request: dict[str, Any]) -> PlanText:
    """A HOST'S VALUE READ AS A RELATION, as a plan (the native library's Boundary.frameLoad; the user's ruling of
    2026-11-11: a frame named, attached at run): ``request`` is ``{"databaseType", "held", "as", "live", "schema"?,
    "relation"}`` -- the name this host holds the frame under, the relation it is attached as, whether it is read at each
    query (a view) or once (a table), and the relation it is read through; never its bytes. The plan: one HOST_SESSION
    load whose attach step ``projects.run_host_load`` runs, binding the value this host holds by that name."""
    return plan_from_answer(_answer(library().call('lite_frame_load', _json.dumps(request))))


def project_data(model: str, database_type: str) -> dict[str, Any]:
    """A project's own database on a database of the given type (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): where
    ``model``'s Data rows live on this host, named by what it holds (the model's Databases and Data rows: an edited row
    is a fresh database). ``name``, and the statements of each step in order: ``open`` makes it, ``use`` points a
    connection of its own at it, ``close`` drops it; and ``load``, the plan whose root is the ONCE load that makes and
    fills every table a relational Data element fills (on that connection; ``projects.run_load``), absent for a model
    with no Data rows (B3, 2026-11-11: until then ``fill``, its statements as text). A database without the operations
    is refused by name."""
    answered = _json.loads(_answer(library().call('lite_project_data', model, database_type)))
    # each step's statements as the planner wrote them (_planned.py); the load, the plan the planner answered (M1),
    # its steps typed as it runs
    load = {} if answered.get('load') is None else {'load': plan_from_answer(_json.dumps(answered['load']))}
    return {**answered, **load, **{step: all_from_plan(answered[step]) for step in ('open', 'use', 'close')}}



def pure_v1(path: str, query: str, body: str) -> Answer:
    """One call of legend-engine's ``pure/v1`` API, answered by legend-lite's server code (``PureV1Api``) --
    every endpoint it serves but execute's run: ``path`` as requested (``/api/pure/v1/...``), ``query`` its raw
    query string (``''`` for none)."""
    return _http(library().call('lite_pure_v1', path, query, body))


def execute_plan(body: str, models: list[str]) -> Answer:
    """The plan half of execute in upstream's Arrow format: for an ``ExecuteInput`` over one of ``models``
    (the only models this host runs), an answer whose body is ``{"sql", "metadata"}`` -- the SQL to run, and
    the Arrow schema metadata upstream's answer carries; any other answer is a refusal, to be sent as it is."""
    return _http(library().call('lite_execute_plan', body, _json.dumps(models)))


def execute_json_plan(body: str, models: list[str]) -> Answer:
    """The plan half of execute in legend-engine's JSON, for a host that runs the SQL on its own database (the tab's, and
    Python's: docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): an answer whose body is ``{"sql", "run", "shape",
    "resultColumns", "builder", ...}`` -- ``run`` the typed statement the server runs, whose rows go back to
    :func:`execute_json_answer`; any other answer is a refusal, to be sent as it is."""
    return _http(library().call('lite_execute_json_plan', body, _json.dumps(models)))


def execute_json_answer(plan: str, rows: str) -> Answer:
    """Execute's answer in legend-engine's JSON: ``plan`` the plan half's body, ``rows`` the rows the database answered
    its ``run`` statement with (a JSON array of objects, each a column's name and its cell as the driver read it:
    :func:`host_rows`), decoded by the plan's columns and written by legend-lite's server's own writer -- the server's
    answer text."""
    return _http(library().call('lite_execute_json_answer', plan, rows))


def refusal(message: str) -> Answer:
    """A database's refusal, as legend-lite's server answers one (``500``, legend-engine's error shape)."""
    return _http(library().call('lite_pure_v1_refusal', message))
