"""The unstructured study's one way to Postgres: pg8000 (BSD-3-Clause), pinned in pins.env.

It replaced psycopg on 2026-11-11 because psycopg is LGPL-3.0-only (tools/unstructured-oracle/README.md, "Licences").
pg8000's DB-API connection has no execute(), so every statement goes through execute() here. Every statement the study
sends is written in pyformat, %(name)s or %s, and pg8000 reads that only when its module-level paramstyle says so: this
module sets it, and every caller imports this module, never pg8000 itself.

A failure the server reports is a DatabaseError carrying the server's own fields: sqlstate() and message() read them.
"""
from __future__ import annotations

from urllib.parse import urlsplit

import pg8000.dbapi

pg8000.dbapi.paramstyle = "pyformat"

Error = pg8000.dbapi.Error
DatabaseError = pg8000.dbapi.DatabaseError
InterfaceError = pg8000.dbapi.InterfaceError


def connect(url: str, *, autocommit: bool):
    """A connection to postgresql://user[:password]@host:port/database, each part but the password given."""
    parts = urlsplit(url)
    database = parts.path[1:]
    if parts.scheme != "postgresql" or not parts.username or not parts.hostname or parts.port is None or not database:
        raise SystemExit(f"a Postgres URL must be postgresql://user@host:port/database, every part given: {url!r}")
    connection = pg8000.dbapi.connect(user=parts.username, password=parts.password, host=parts.hostname,
                                      port=parts.port, database=database)
    connection.autocommit = autocommit
    return connection


def execute(connection, sql: str, parameters):
    """The cursor that ran sql with parameters (a dict for %(name)s, a tuple for %s, () for none)."""
    cursor = connection.cursor()
    cursor.execute(sql, parameters)
    return cursor


def sqlstate(failure: DatabaseError) -> str:
    """The SQLSTATE the server sent, e.g. 42P01."""
    return failure.args[0]["C"]


def message(failure: DatabaseError) -> str:
    """The server's primary message, as it sent it."""
    return failure.args[0]["M"]
