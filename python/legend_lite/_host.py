"""A HOST'S NAMED OPERATIONS on Python's DuckDB (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5), the twin of the tab's
(engine-client/src/host-operations.ts): what Python's host used to spell as SQL itself -- a frame's table or view, a
copy and its schema, a catalog question, an extension's settings -- named here with its data, and answered by the
native library (``compiler.host_operations``: the platform's HostOperations in Java, by the database's type) as the
statements DuckDB runs. Python spells none. Each operation is the JSON the tab's sends (its types:
host-operations.ts)."""

from __future__ import annotations

from typing import Any

from . import compiler
from ._planned import PlannedSql, send

DUCKDB = 'DuckDB'


def name(table: str, *, schema: str | None = None, database: str | None = None) -> dict[str, Any]:
    """A relation's name: a person's or a host's (a frame's, a copy's), quoted by the dialect."""
    return {'name': table, **({'schema': schema} if schema is not None else {}),
            **({'database': database} if database is not None else {})}


def statements(*operations: dict[str, Any], database_type: str = DUCKDB) -> list[PlannedSql]:
    """The operations' statements, in order."""
    return [sql for answered in compiler.host_operations(database_type, list(operations)) for sql in answered]


def run(connection: Any, *operations: dict[str, Any]) -> None:
    """The operations run on ``connection``, in order."""
    for sql in statements(*operations):
        send(connection, sql)


def ask(connection: Any, operation: dict[str, Any]) -> list[tuple[Any, ...]]:
    """A question's rows (a catalog question, a count): its one statement run on ``connection``."""
    (sql,) = statements(operation)
    return send(connection, sql).fetchall()


def count(connection: Any, operation: dict[str, Any]) -> int:
    """A counting question's number (its one row's one column)."""
    return int(ask(connection, operation)[0][0])
