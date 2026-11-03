"""legend-lite's compiler, in Python: the same compiler the browser runs as WebAssembly, built native.

It works on PROTOCOL TREES (upstream's V1 lambda JSON, as Python dicts), the way DataCube builds its
queries: Pure text is one way to make a tree (``parse``), and a tree prints back as Pure
(``print_tree``). Typing and planning take a model's text, compiled for the call; a ``HeldModel`` is a model
compiled once and planned against many times, for as long as it is open.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import _json
from ._library import library

Tree = dict[str, Any]


class LegendError(Exception):
    """The compiler refused: what it said, and the kind of refusal, by one rule for every call: legend-engine's
    ``errorType`` when the text is refused (``PARSER`` when it does not parse, ``COMPILATION`` when it does not
    compile or uses a construct not implemented), else the status legend-lite's server answers (``'500'``), the
    message then led by the failure's class, as the server writes it."""

    def __init__(self, message: str, kind: str) -> None:
        super().__init__(message)
        self.message = message
        self.kind = kind


@dataclass(frozen=True)
class Column:
    """A result column as the compiler typed it."""

    name: str
    type: str
    raw: dict[str, Any] = field(hash=False, compare=False)

    @property
    def sql_type(self) -> str | None:
        """Its SQL type, when the plan says it (a server's ``tdsColumns`` carry ``relationalType``: the one typing
        table's pair, the program's G9)."""
        found = self.raw.get('relationalType') if isinstance(self.raw, dict) else None
        return found if isinstance(found, str) else None


@dataclass(frozen=True)
class Answer:
    """An HTTP answer of legend-engine's ``pure/v1`` API, as legend-lite's server gives it: its status, media
    type and body."""

    status: int
    content_type: str
    body: str


@dataclass(frozen=True)
class Plan:
    """A query planned: the SQL to run, and the compiler's type of what it returns."""

    sql: str
    columns: tuple[Column, ...]


def _answer(text: str) -> str:
    """The compiler's answer, or its refusal raised (its kind as the server names it: ``planner.Folded``)."""
    tag, _, rest = text.partition('\n')
    if tag == 'OK':
        return rest
    if tag == 'ERR':
        kind, _, message = rest.partition('\n')
        raise LegendError(message or 'the compiler refused it', kind)
    raise OSError(f'the native library answered something unrecognised: {text[:120]!r}')


def _http(text: str) -> Answer:
    status, content_type, body = _answer(text).split('\n', 2)
    return Answer(int(status), content_type, body)


def _columns(relation_type: dict[str, Any]) -> tuple[Column, ...]:
    return tuple(Column(c['name'], c['genericType']['rawType']['fullPath'], c) for c in relation_type['columns'])


def _engine(path: str, query: str, body: str) -> str:
    """One ``pure/v1`` call's body, as legend-lite's server answers it; a refusal raised, its kind the engine's
    ``errorType``, else the answer's status."""
    answer = pure_v1(path, query, body)
    if answer.status == 200:
        return answer.body
    try:
        refusal = _json.loads(answer.body)
    except ValueError:
        raise LegendError(answer.body or 'the compiler refused it', str(answer.status)) from None
    message = refusal.get('message') if isinstance(refusal, dict) else None
    kind = refusal.get('errorType') if isinstance(refusal, dict) else None
    raise LegendError(message or 'the compiler refused it', kind or str(answer.status))


def parse(text: str) -> Tree:
    """Pure text as its lambda's protocol tree, e.g. ``parse("|1 + 1")`` (``grammar/grammarToJson/lambda``)."""
    return _json.loads(_engine('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', text))


def print_tree(tree: Tree, style: str = 'PRETTY') -> str:
    """A lambda's tree as Pure text: ``PRETTY`` across lines, or ``STANDARD`` on one (``grammar/jsonToGrammar/lambda``)."""
    if style not in ('PRETTY', 'STANDARD'):
        raise ValueError(f"style must be 'PRETTY' or 'STANDARD', not {style!r}")
    return _engine('/api/pure/v1/grammar/jsonToGrammar/lambda', f'renderStyle={style}', _json.dumps(tree))


def relation_type(model: str, tree: Tree) -> tuple[Column, ...]:
    """The columns a relation query returns, as the compiler types them, compile-only
    (``compilation/lambdaRelationType``)."""
    request = {'model': {'_type': 'text', 'code': model}, 'lambda': tree}
    return _columns(_json.loads(_engine('/api/pure/v1/compilation/lambdaRelationType', '', _json.dumps(request))))


def read_planned(answer: str) -> Plan:
    """The planner's tagged answer (``OK`` and its plan, or ``ERR``, its kind and message) as a Plan, its refusal raised
    (``LegendError``): what the plan function the tab's worker calls answers (``lite_plan_json_held``)."""
    return read_plan(_answer(answer))


def read_plan(answer: str) -> Plan:
    """The planner's plan (the plan record, the answer after its ``OK`` tag: one typed result over a statement of
    text, the one plan every host path runs, docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R2) as a Plan: its statement,
    and its columns, each typed by the record's one rule (:func:`plain_type`) -- the one reading of it, as DataCube's
    planner reads it (lite-planner.ts ``readPlan``). A plan of another format, version or shape is refused by name."""
    root = _record_root(answer)
    return Plan(root['sql']['statement'], tuple(Column(c['name'], plain_type(c['type']), c) for c in root['columns']))


def plain_type(type_name: str) -> str:
    """A plan record's column type as legend-engine's relation type names it, by the record's one rule
    (``ExecutionPlan.Column.plainType``; engine-client's plan-record.ts ``plainType``), read where every type name is
    (:func:`legend_lite._types.record_type`)."""
    from ._types import record_type
    return record_type(type_name)


def plan(model: str, tree: Tree, runtime: str) -> Plan:
    """A relation query planned against ``model`` through ``runtime``: its SQL and its columns."""
    return read_plan(_answer(library().call('lite_plan_json', model, _json.dumps(tree), runtime)))


def plan_text(model: str, text: str, runtime: str) -> Plan:
    """The same, from Pure text."""
    return read_plan(_answer(library().call('lite_plan_text', model, text, runtime)))


class HeldModel:
    """A model the compiler holds: compiled once, when it is made, and planned and typed against as often as asked,
    each time paying for the query alone (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §9). The same text held twice is
    the one model, compiled once. Held until ``close()`` (or the end of a ``with`` block); a model that does not compile
    is refused when it is made, and nothing is held. Safe from several threads: one plans against it at a time."""

    def __init__(self, model: str) -> None:
        self.model = model
        self._handle: str | None = _answer(library().call('lite_hold_model', model))

    def _held(self) -> str:
        if self._handle is None:
            raise ValueError('this model was let go (closed): hold it again')
        return self._handle

    @property
    def handle(self) -> str:
        """The library's handle of the model held: what a planner request of a held kind names (legend_lite._planner)."""
        return self._held()

    def plan(self, tree: Tree, runtime: str) -> Plan:
        """A relation query planned against the model held, through ``runtime``: its SQL and its columns."""
        return read_plan(_answer(library().call('lite_plan_json_held', self._held(), _json.dumps(tree), runtime)))

    def relation_type(self, tree: Tree) -> tuple[Column, ...]:
        """The columns a relation query returns, typed against the model held, compile-only."""
        return _columns(_json.loads(_answer(library().call('lite_relation_type_held', self._held(), _json.dumps(tree)))))

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
    """A model's text as its elements (PureModelContextData), as the compiler reads them (``grammar/grammarToJson/model``)."""
    return _json.loads(_engine('/api/pure/v1/grammar/grammarToJson/model', 'returnSourceInformation=false', text))['elements']


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
    rows]}``. Returns ``{"model", "runtime", "source", "accessor", "conversions", "copySelectList"?, "excluded",
    "bitColumns"}``: ``model`` is Pure text, ``source`` the relation that reads the table (a tree; ``accessor`` its
    text), ``copySelectList`` the select list the table applies, rewritten in place so it holds the declared types,
    written by its own database (``convertible``; absent when the table is never rewritten).

    Given ``{"databaseType", "connection"? (default "host"), "tables": [{"table", "schema"?, "pkg"? (default
    "local"), "convertible", "columns"}]}`` instead, the model of SEVERAL tables of one database (a join's inputs, read
    in one place): each package's Database declaring its tables as each one's own model does, one connection, and a
    runtime binding every Database to it. Returns ``{"model", "runtime", "tables": [{"source", "accessor",
    "conversions", "excluded"}]}``, the tables in the order given; a table given twice is refused."""
    return _json.loads(_answer(library().call('lite_table_model', _json.dumps(table))))


def catalog_columns_sql(schema: str, table: str) -> str:
    """The catalog question for one table of a DuckDB: SQL whose rows (``column_name``, ``data_type``,
    ``logical_type``, ``numeric_precision``, ``numeric_scale``, ``not_null``) describe its columns."""
    return _answer(library().call('lite_catalog_columns_sql', schema, table))


def session_setup(database_type: str) -> list[str]:
    """What a session of the given database runs before it is queried, so that it answers as the planner's
    SQL expects: the dialect's own setup (a DuckDB session in UTC)."""
    return _json.loads(_answer(library().call('lite_session_setup', database_type)))


def host_operations(database_type: str, operations: list[dict[str, Any]]) -> list[list[str]]:
    """A host's named operations on its own database of the given type (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5):
    each operation's statements, in order -- what Python's host runs instead of spelling SQL (engine-client's
    host-operations.ts writes the same operations). One the database has not is refused by name."""
    return _json.loads(_answer(library().call('lite_host_operations', database_type, _json.dumps(operations))))


def project_data(model: str, database_type: str) -> dict[str, Any]:
    """A project's own database on a database of the given type (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): where
    ``model``'s Data rows live on this host, named by what it holds (the model's Databases and Data rows: an edited row
    is a fresh database). ``name``, and the statements of each step in order: ``open`` makes it, ``use`` points a
    connection of its own at it, ``fill`` makes and fills every table a relational Data element fills (on that
    connection), ``close`` drops it. A database without the operations is refused by name."""
    return _json.loads(_answer(library().call('lite_project_data', model, database_type)))



def pure_v1(path: str, query: str, body: str) -> Answer:
    """One call of legend-engine's ``pure/v1`` API, answered by legend-lite's server code (``PureV1Api``) --
    every endpoint it serves but execute's run: ``path`` as requested (``/api/pure/v1/...``), ``query`` its raw
    query string (``''`` for none)."""
    return _http(library().call('lite_pure_v1', path, query, body))


def execute_json_plan(body: str, models: list[str]) -> Answer:
    """The plan of execute in legend-engine's JSON, for a host that runs the SQL on its own database (the tab's, and
    Python's: docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): an answer whose body is the plan record (PlanJson's text, the
    one plan every host path runs: docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R2), its one typed result the statement
    to run (:func:`answer_statement`), whose rows go back to :func:`execute_json_answer` with it; any other answer is a
    refusal, to be sent as it is."""
    return _http(library().call('lite_execute_json_plan', body, _json.dumps(models)))


# The execution plan's format a host reads (core/.../executionplan/PlanJson.java VERSION, its one owner; the tab's twin
# engine-client/src/plan-record.ts): 20 since 2026-11-02, each column's enumeration fact (R2, D16).
PLAN_VERSION = 20


def answer_statement(plan: str) -> str:
    """The one statement a host runs for a query's rows (the plan record's one typed result: ``HostPlans.read``, and
    ``PureV1Api.jsonPlan``'s, anything else refused there, at planning): its text. A plan of another format, version or
    shape is refused by name (``ValueError``), never run some other way."""
    return _record_root(plan)['sql']['statement']


def _record_root(plan: str) -> dict[str, Any]:
    """A plan record's one typed result over a statement of text, its format and version this host's; anything else
    refused by name (``ValueError``)."""
    read = _json.loads(plan)
    if not isinstance(read, dict) or read.get('format') != 'legend-lite-plan' or read.get('version') != PLAN_VERSION:
        said = read if not isinstance(read, dict) else {'format': read.get('format'), 'version': read.get('version')}
        raise ValueError(f"not a legend-lite execution plan of version {PLAN_VERSION}: {_json.dumps(said)}")
    root = read.get('root')
    sql = root.get('sql') if isinstance(root, dict) else None
    # as the tab reads it (plan-record.ts hostResult, relation-type.ts recordColumn; the R2 audit, 2026-11-03): one
    # typed result over a statement of text, its slots an empty list, each column a name, a type and its enumeration
    # fact stated (null: none the compiler typed, which a host's reader refuses)
    if not isinstance(root, dict) or root.get('_type') != 'tdsResult' or not isinstance(sql, dict) \
            or not isinstance(sql.get('statement'), str) or sql.get('slots') != [] \
            or not isinstance(root.get('columns'), list):
        kind = root.get('_type') if isinstance(root, dict) else root
        raise ValueError(f"a host's plan is one typed result over a statement of text, not a {kind}")
    for c in root['columns']:
        if not isinstance(c, dict) or not isinstance(c.get('name'), str) or not isinstance(c.get('type'), str) \
                or not isinstance(c.get('enumeration'), bool):
            raise ValueError(f"a plan's column without a name, a type and its enumeration fact: {_json.dumps(c)}")
    return root


def execute_json_answer(plan: str, rows: str) -> Answer:
    """Execute's answer in legend-engine's JSON: ``plan`` the plan's body (the plan record), ``rows`` the rows the database
    answered its statement with (a JSON array of objects, each a column's name and its cell as the driver read it:
    :func:`host_rows`), decoded by the plan's columns and written by legend-lite's server's own writer -- the server's
    answer text."""
    return _http(library().call('lite_execute_json_answer', plan, rows))


def _http_bytes(data: bytes) -> tuple[int, str, bytes]:
    """A bytes answer (``planner.Folded.httpBytes``): its status, media type and body bytes; a refusal raised."""
    if data.startswith(b'OK\n'):
        status, content_type, body = data[3:].split(b'\n', 2)
        return int(status), content_type.decode('utf-8'), body
    _answer(data.decode('utf-8'))
    raise OSError(f'the native library answered something unrecognised: {data[:120]!r}')


def execute_json_answer_arrow(plan: str, arrow: Any) -> Answer:
    """:func:`execute_json_answer` from the rows as ARROW (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R3): ``arrow`` the
    Arrow IPC stream the database answered the plan's statement with (a pyarrow ``Buffer`` or ``bytes``), handed to
    the library where it lies and read there -- no row becomes a Python object or text on the way."""
    status, content_type, body = _http_bytes(library().call_bytes('lite_execute_json_answer_arrow', plan, arrow))
    return Answer(status, content_type, body.decode('utf-8'))


def execute_arrow_answer_arrow(plan: str, reported: str, arrow: Any) -> tuple[int, str, bytes]:
    """Execute's answer in upstream's Arrow format from the rows as ARROW (R3), written as legend-engine writes it
    (R4 step 3: each field typed from ``reported``, the database's own ``[label, type]`` pairs, read as DuckDB's JDBC
    driver reports them): its status, media type and body (the zstd frame upstream sends, or a refusal's JSON)."""
    return _http_bytes(library().call_bytes('lite_execute_arrow_answer_arrow', (plan, reported), arrow))


def execute_lite_answer_arrow(plan: str, arrow: Any) -> tuple[int, str, bytes]:
    """Lite's own execute's answer (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md D20: ``/api/lite/v1/execution/execute``)
    from the rows as ARROW: lite's own Arrow in the plan's types, the plan record in its schema's metadata
    (``legend.plan``) -- its status, media type and body (the Arrow stream, or a refusal's JSON)."""
    return _http_bytes(library().call_bytes('lite_execute_lite_answer_arrow', plan, arrow))


def keyed_lite_answer_arrow(plan: str, keys: str, arrow: Any) -> tuple[int, str, bytes]:
    """:func:`execute_lite_answer_arrow` for a keyed plan (a pivot whose columns its data names): ``keys`` the keys
    statement's rows (as :func:`keyed_answer` takes them), ``arrow`` the Arrow the body written for them answered."""
    return _http_bytes(library().call_bytes('lite_keyed_lite_answer_arrow', (plan, keys), arrow))


def execute_refusal(plan: str, message: str, trace: str) -> Answer:
    """:func:`execute_json_plan`'s statement refused by the host's database, answered as legend-engine answers a
    database's refusal at run time (D18 B: ``PureV1Api.databaseRefused``): ``message`` the database's words, ``trace``
    how this host received them (register S129)."""
    return _http(library().call('lite_execute_refusal', plan, message, trace))


def keyed_statement(plan: str) -> str | None:
    """A pivot's keys statement when :func:`execute_json_plan`'s plan is a keyed one (a pivot whose columns its data
    names, D18 D): the statement a host runs first; None for a plan of one statement."""
    read = _json.loads(plan)
    root = read.get('root') or {}
    if root.get('_type') != 'keyed':
        return None
    keys = root.get('keys') or {}
    if not isinstance(keys.get('statement'), str) or keys.get('slots'):
        raise ValueError("a host's pivot reads its keys with a statement of text")
    return keys['statement']


def keyed_body(plan: str, keys: str) -> Answer:
    """A pivot's body for the keys a host read (``keys``: a JSON array of rows, each the texts the keys statement
    wrote): ``{"statement", "parameters"}``, the body and the text each placeholder binds (``PureV1Api.keyedBody``)."""
    return _http(library().call('lite_keyed_body', plan, keys))


def keyed_answer(plan: str, keys: str, rows: str, reported: str) -> Answer:
    """A pivot's answer in legend-engine's JSON from the body's rows (:func:`host_rows`) and each body column's type as
    the database reported it (``reported``: a JSON array of DuckDB's type names, a ``DESCRIBE`` of the body)."""
    return _http(library().call('lite_keyed_answer', plan, keys, rows, reported))


def host_rows(names: list[str], rows: list[tuple]) -> str:
    """A result's rows as the compiler reads a host's rows back (``PureV1Api.jsonAnswer``): a JSON array of objects,
    each a column's name and its cell exactly as the driver read it -- an integer's digits, a float's shortest digits
    (a non-finite one as its name in a string), a decimal, a date, a time and a date-time as their text, a text, a
    boolean, null, a structure as its JSON with the same rules. The plan's columns say how each reads."""
    return '[' + ','.join('{' + ','.join(_json.dumps(n) + ':' + _cell(v) for n, v in zip(names, row)) + '}'
                          for row in rows) + ']'


def _cell(v) -> str:
    import datetime
    import decimal
    import math
    import uuid
    if v is None:
        return 'null'
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isfinite(v):
            return repr(v)
        return _json.dumps('NaN' if math.isnan(v) else ('Infinity' if v > 0 else '-Infinity'))
    if isinstance(v, decimal.Decimal):
        return _json.dumps(str(v))
    if isinstance(v, datetime.datetime):
        # a zoned value at UTC, its offset dropped: the plan's session is UTC (Boundary.sessionSetup)
        if v.tzinfo is not None:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return _json.dumps(v.isoformat())
    if isinstance(v, (datetime.date, datetime.time)):
        return _json.dumps(v.isoformat())
    if isinstance(v, str):
        return _json.dumps(v)
    if isinstance(v, uuid.UUID):
        return _json.dumps(str(v))
    if isinstance(v, (list, tuple)):
        return '[' + ','.join(_cell(x) for x in v) + ']'
    if isinstance(v, dict):
        return '{' + ','.join(_json.dumps(str(k)) + ':' + _cell(x) for k, x in v.items()) + '}'
    raise TypeError(f'a cell of {type(v).__name__} has no form a host hands its rows back in')


def refusal(message: str) -> Answer:
    """A database's refusal, as legend-lite's server answers one (``500``, legend-engine's error shape)."""
    return _http(library().call('lite_pure_v1_refusal', message))
