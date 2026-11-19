"""legend-lite's compiler, in Python: the same compiler the browser runs as WebAssembly, built native.

It works on PROTOCOL TREES (upstream's V1 lambda JSON, as Python dicts), the way DataCube builds its
queries: Pure text is one way to make a tree (``parse``), and a tree prints back as Pure
(``print_tree``). Typing and planning take a model's text, compiled for the call; a ``HeldModel`` is a model
compiled once and planned against many times, for as long as it is open.
"""

from __future__ import annotations

from typing import Any

from . import _folded, _json
from ._library import library
from ._planned import PlannedSql, PlanText, all_from_plan, from_plan, plan_from_answer
from ._raised import declare_sites
# the plan's records and its own data's readers, theirs since C5-P (2026-11-15): named here as they always were
from ._plans import (Column, Plan, PlanParamsRefused, host_rows, plan_binding, plan_params,  # noqa: F401
                     statement_params)

Tree = dict[str, Any]


# lite's one error shape, the seam's (_folded: the native library's answers, read once; H4's P3, 2026-11-19), named here
# as every caller always took it
LegendError = _folded.LegendError


# lite's error kinds by name, each its wire text, and the HTTP status a refusal of each kind is answered with, as the
# product says them (``lite_refusal_kinds``), each read once
_KINDS: dict[str, str] | None = None
_STATUSES: dict[str, int] | None = None


def _said_kinds() -> dict[str, dict[str, Any]]:
    """LITE'S ERROR KINDS as the product says them (``lite_refusal_kinds``: the one Java type of §3's kinds,
    ``LiteV1Api.Kind`` -- each name, its wire text and its status)."""
    try:
        return _json.loads(_folded.unfold(library().call('lite_refusal_kinds')))
    except LegendError as refused:
        raise OSError(f"the native library did not say lite's error kinds: {refused.message[:200]}") from None


def refusal_kinds() -> dict[str, str]:
    """LITE'S ERROR KINDS, by name, as the product names them (``lite_refusal_kinds``), each name and its wire text, read
    once: a refusal's ``kind`` is compared with these, never with a spelling of Python's own, so a rename in the
    product needs no edit here (the lite API spec's §3, 2026-11-17)."""
    global _KINDS
    if _KINDS is None:
        _KINDS = {name: said['wire'] for name, said in _said_kinds().items()}
    return _KINDS


def refusal_status(kind: str) -> int:
    """The HTTP status a refusal of ``kind`` (its wire text) is answered with -- the product's (``LiteV1Api.Kind``,
    through ``lite_refusal_kinds``), read once, never a rule of Python's own (H4's P2, the user's CP-13 of 2026-11-19:
    until then a ``notFound`` was answered 400 here). A kind the product does not say is refused by name."""
    global _STATUSES
    if _STATUSES is None:
        _STATUSES = {said['wire']: said['status'] for said in _said_kinds().values()}
    if kind not in _STATUSES:
        raise LegendError(f'an error kind this binding does not know: {kind}', error_kind('INTERNAL'))
    return _STATUSES[kind]


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


def _answer(text: str) -> str:
    """The compiler's answer, or its refusal raised in lite's one error shape, read by the one reader (``_folded``:
    ``OK`` and the answer, or ``ERR`` and §3's ``{kind, message, where?, raise?}``). A kind the product does not say is
    refused by name (``_known``)."""
    try:
        return _folded.unfold(text)
    except LegendError as refused:
        raise _known(refused) from None


def _known(refused: LegendError) -> LegendError:
    """``refused``, its kind one the product says; any other refused by name, kind internal."""
    if refused.kind not in refusal_kinds().values():
        return LegendError(f'an error kind this binding does not know: {refused.kind}', error_kind('INTERNAL'))
    return refused


def folded(text: str) -> str:
    """The native library's fold read as this binding reads every answer (``OK`` and the answer, or ``ERR`` and lite's
    error raised): for the edge's entries, which answer in legend-engine's shape (legend_lite.edge)."""
    return _answer(text)


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
    that does is refused by name (kind ``request``), in the server's words (``lite_route``: H4's P4, 2026-11-19)."""
    status, said = lite_route(_USER, '' if request is None else _json.dumps(request))
    answer = _json.loads(said)
    if status != 200:
        raise LegendError(answer['message'], answer['kind'], answer.get('where'), answer.get('raise'))
    return answer


# lite's user operation's route (the spec's §2; LiteV1Api.USER)
_USER = '/api/lite/v1/user'


def lite_route(path: str, body: str, method: str = 'POST') -> tuple[int, str]:
    """LITE'S OWN API, v1, AS A ROUTE (the spec's §2, §3, §5; the host train, 2026-11-17): a call of
    ``/api/lite/v1/<operation>`` -- its method, path and request JSON -- answered as the server's routes answer it, by
    the product's router through one native entry (``lite_route``: ``Boundary.liteRoute``, H4's P4, 2026-11-19): 200 and
    the answer's JSON, or §3's error ``{kind, message, where?, raise?}`` with its kind's status -- its words, its kind
    and its status Java's, never Python's (until then an unknown path's and a malformed request's words were Python's,
    and a GET was answered 404 in text). ``user`` answers this user (_home.THIS_USER). query/run is the host's own,
    answered before this (Engine._query_run). The one rule Python's server and DataCube's code in this process
    (_headless) answer by."""
    from ._home import THIS_USER
    status, _, said = answered_http(library().call('lite_route', method, path, body, THIS_USER))
    return status, said


def answered_http(text: str) -> tuple[int, str, str]:
    """A native entry's answer folded as an HTTP answer (``planner.Folded.http``), its status, media type and body, read
    by the one reader (``_folded.unfold_http``); a refusal folded in its place (the call failed outside the route's own
    answer) raised as ``LegendError``, its kind checked as ``_answer`` checks one."""
    try:
        return _folded.unfold_http(text)
    except LegendError as refused:
        raise _known(refused) from None


def refused_route(refused: LegendError) -> tuple[int, str]:
    """A refusal as lite's route answers it (the spec's §3): ``{kind, message, where?, raise?}`` and its kind's status,
    the product's (``refusal_status``)."""
    said: dict[str, Any] = {'kind': refused.kind, 'message': refused.message}
    if refused.where is not None:
        said['where'] = refused.where
    if refused.raise_ is not None:
        said['raise'] = refused.raise_
    return refusal_status(refused.kind), _json.dumps(said)


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
