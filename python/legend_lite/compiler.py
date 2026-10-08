"""legend-lite's compiler, in Python: the same compiler the browser runs as WebAssembly, built native.

It works on PROTOCOL TREES (upstream's V1 lambda JSON, as Python dicts), the way DataCube builds its
queries: Pure text is one way to make a tree (``parse``), and a tree prints back as Pure
(``print_tree``). Typing and planning take a model's text; the compiler keeps what it built for a model
it has seen, so passing the same text again costs nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import _json
from ._library import library

Tree = dict[str, Any]


class LegendError(Exception):
    """The compiler refused: what it said, and the kind of refusal (its Java exception class)."""

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


@dataclass(frozen=True)
class Plan:
    """A query planned: the SQL to run, and the compiler's type of what it returns."""

    sql: str
    columns: tuple[Column, ...]


def _answer(text: str) -> str:
    """The compiler's answer, or its refusal raised."""
    tag, _, rest = text.partition('\n')
    if tag == 'OK':
        return rest
    if tag == 'ERR':
        kind, _, message = rest.partition('\n')
        raise LegendError(message or 'the compiler refused it', kind)
    raise OSError(f'the native library answered something unrecognised: {text[:120]!r}')


def _columns(relation_type: dict[str, Any]) -> tuple[Column, ...]:
    return tuple(Column(c['name'], c['genericType']['rawType']['fullPath'], c) for c in relation_type['columns'])


def parse(text: str) -> Tree:
    """Pure text as its lambda's protocol tree, e.g. ``parse("|1 + 1")``."""
    return _json.loads(_answer(library().call('lite_lambda_json', text)))


def print_tree(tree: Tree, style: str = 'PRETTY') -> str:
    """A lambda's tree as Pure text: ``PRETTY`` across lines, or ``STANDARD`` on one."""
    if style not in ('PRETTY', 'STANDARD'):
        raise ValueError(f"style must be 'PRETTY' or 'STANDARD', not {style!r}")
    return _answer(library().call('lite_compose', _json.dumps(tree), style))


def relation_type(model: str, tree: Tree) -> tuple[Column, ...]:
    """The columns a relation query returns, as the compiler types them, compile-only."""
    return _columns(_json.loads(_answer(library().call('lite_relation_type_json', model, _json.dumps(tree)))))


def plan(model: str, tree: Tree, runtime: str) -> Plan:
    """A relation query planned against ``model`` through ``runtime``: its SQL and its columns."""
    out = _json.loads(_answer(library().call('lite_plan_json', model, _json.dumps(tree), runtime)))
    return Plan(out['sql'], _columns(out['type']))


def plan_text(model: str, text: str, runtime: str) -> Plan:
    """The same, from Pure text."""
    out = _json.loads(_answer(library().call('lite_plan_text', model, text, runtime)))
    return Plan(out['sql'], _columns(out['type']))


def model_elements(text: str) -> list[dict[str, Any]]:
    """A model's text as its elements (PureModelContextData), as the compiler reads them."""
    return _json.loads(_answer(library().call('lite_model_json', text)))['elements']


def database_from_catalog(catalog: dict[str, Any]) -> dict[str, Any]:
    """A Pure Database for one table, written from its catalog rows, as DataCube writes one for a file.

    Takes ``{"path", "schema"?, "table", "convertible", "databaseType", "columns": [{"name", "dataType",
    "logicalType", ...}]}``, where ``databaseType`` is the database whose catalog it is (e.g. ``"DuckDB"``).
    Returns ``{"text", "source", "conversions", "excluded"}``: ``text`` is the Database's Pure text."""
    return _json.loads(_answer(library().call('lite_database_from_catalog', _json.dumps(catalog))))


def table_model(table: dict[str, Any]) -> dict[str, Any]:
    """The model for a table, from its catalog rows: the Database, its connection and runtime -- the one
    writer DataCube's tables and Python's frames share (the compiler's ``tableModelOrError``).

    Takes ``{"table", "schema"?, "pkg"? (default "local"), "convertible", "databaseType",
    "snapDatabaseType"?, "columns": [catalog rows]}``. Returns ``{"model", "runtime", "snapRuntime"?,
    "source", "accessor", "conversions", "copySelectList", "excluded", "bitColumns"}``: ``model`` is Pure
    text, ``source`` the relation that reads the table (a tree; ``accessor`` its text), ``copySelectList``
    the select list a copy applies so it holds the declared types."""
    return _json.loads(_answer(library().call('lite_table_model', _json.dumps(table))))


def catalog_columns_sql(schema: str, table: str) -> str:
    """The catalog question for one table of a DuckDB: SQL whose rows (``column_name``, ``data_type``,
    ``logical_type``, ``numeric_precision``, ``numeric_scale``, ``not_null``) describe its columns."""
    return _answer(library().call('lite_catalog_columns_sql', schema, table))


def session_setup(database_type: str) -> list[str]:
    """What a session of the given database runs before it is queried, so that it answers as the planner's
    SQL expects: the dialect's own setup (a DuckDB session in UTC)."""
    return _json.loads(_answer(library().call('lite_session_setup', database_type)))

