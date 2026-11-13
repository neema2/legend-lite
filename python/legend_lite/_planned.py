"""THE ONE TEXT PYTHON'S HOST SENDS ITS DATABASE (the total wall's C3, 2026-11-12, the user's: "the hosts' separation
proven structurally"), the twin of engine-client's planned-sql.ts: a statement a PLAN carries, or one the PLANNER wrote
as its answer (a host operation's, a session's setup, a catalog's read, a project database's statements, a load's steps).

``PlannedSql`` is a ``str`` made in ONE place each, where a plan or a planner's answer is READ (``from_plan``'s callers:
compiler.py's readers, projects.py's load runners, engine.py's execute plan); ``send`` -- every statement Python's host
runs on its database goes through it -- refuses any other text by name (``NotPlanned``). A ``str`` edited after it was
read (concatenated, stripped, formatted) is a plain ``str`` again, so an edited plan statement is refused too.

No other text reaches ``send``: Python spells no SQL of its own, and DataCube's own code, hosted in V8 (``_headless``),
hands Python's database no statement and no plan -- it asks for what it wants (a load, operations, a catalog, a table
made, a write), which Python's planner plans and its runner runs (the #6 bridge, the user's go of 2026-11-12: until then
the statements DataCube's code had planned crossed the bridge as text and were admitted by the set its planner had
answered, ``across_the_bridge`` and ``Answered``, both deleted). The guard (python/tests/test_planned_guard.py) pins it.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from typing import Any


class PlannedSql(str):
    """A statement a plan carries or the planner wrote: the only text ``send`` runs."""

    __slots__ = ()


class NotPlanned(TypeError):
    """A text sent to a database that no plan carries: refused, never run."""


class PlanText(str):
    """A PLAN, AS THE PLANNER ANSWERED IT (PlanJson's text; the re-audit's M1, 2026-11-12: the runners took any dict, so a
    plan a host wrote ran, its statements typed as planned by the runner that read them). The hosts' runners
    (projects.run_load, run_host_load) take this and no other; it is made where the compiler reads the planner's answer
    (:func:`plan_from_answer`, compiler.py), and it is text, so editing it makes a plain string again."""

    __slots__ = ()


def plan_from_answer(text: str) -> PlanText:
    """``text``, the plan the planner answered, as its answer was READ -- called by the compiler's readers alone (the
    guard, python/tests/test_planned_guard.py, holds it so)."""
    if type(text) is not str:
        raise TypeError(f"a planner's answer is text, not {type(text).__name__}")
    return PlanText(text)


def read_plan(plan: Any, where: str) -> dict:
    """``plan``'s JSON, if it is a plan the planner answered; else refused by name, ``where`` naming what was to run it."""
    if type(plan) is not PlanText:
        raise NotPlanned(f"not a plan the planner answered, so not run by {where}: a {type(plan).__name__} (a host runs only"
                         ' what the planner answered: _planned.py)')
    return json.loads(plan)


def from_plan(text: str) -> PlannedSql:
    """``text`` as the plan or the planner's answer it was READ from carries it: called by the plan readers alone (the
    guard, tools/guards/PlannedSqlTest.java, holds it so). The text is never edited, only typed."""
    if not isinstance(text, str):
        raise NotPlanned(f"a plan's statement is text, not {type(text).__name__}")
    return text if isinstance(text, PlannedSql) else PlannedSql(text)


def all_from_plan(texts: Iterable[str]) -> list[PlannedSql]:
    """Each of ``texts``, as :func:`from_plan` types one."""
    return [from_plan(t) for t in texts]


def host_operation_refusal(asked: Any) -> str | None:
    """Why a request DataCube's hosted code makes of the planner is refused, or None: a host operation takes NO SQL (the
    final audit's item 1, 2026-11-12, the user's ruling: no planner door takes SQL but the playground's) -- a source
    ``query``, a ``selectList``, a ``replacing`` expression, each refused by name here, the planner never asked (the
    platform refuses each too: HostOperationsJson). Kept from ``Answered.refusal`` when the bridge on intent deleted the
    set it lived on (the DataCube stream's merge of the #6 bridge, 2026-11-13)."""
    if not isinstance(asked, dict) or asked.get('kind') != 'hostOperations':
        return None
    operations = asked.get('operations')
    for op in json.loads(operations) if isinstance(operations, str) else []:
        source = op.get('from') if isinstance(op, dict) else None
        keys = ['query'] if isinstance(source, dict) and 'query' in source else []
        keys += [k for k in ('selectList', 'replacing') if isinstance(op, dict) and k in op]
        if keys:
            return (f"a host operation ({op.get('op')}) takes no SQL, and its {keys[0]} is SQL: a query is planned from"
                    ' its pull, a copy\'s conversions are the platform\'s from its columns\' catalog (convert)'
                    ' (_planned.py host_operation_refusal)')
    return None


def require(statement: Any, where: str) -> PlannedSql:
    """``statement`` if it is a plan's; else refused by name, ``where`` saying what it was about to be sent to. The type
    exactly (2026-11-12, the re-audit's S4: an isinstance check passed a str subclass whose ``__class__`` claimed to be
    PlannedSql)."""
    if type(statement) is PlannedSql:
        return statement
    shown = statement if isinstance(statement, str) else type(statement).__name__
    if isinstance(shown, str) and len(shown) > 120:
        shown = shown[:117] + '...'
    raise NotPlanned(f"not a plan's statement, so not sent to {where}: {shown!r} (a host sends only what a plan "
                     'carries or the planner wrote: _planned.py)')


_PLAIN_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')


class Result:
    """A statement's answer as the host reads it: its rows, its Arrow table, its columns' description -- never the
    connection DuckDB answers with, which runs any text (``execute`` returns it)."""

    __slots__ = ('_answer',)

    def __init__(self, answer: Any) -> None:
        self._answer = answer

    @property
    def description(self) -> Any:
        return self._answer.description

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._answer.fetchall()

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._answer.fetchone()

    def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        return self._answer.fetchmany(size)

    def to_arrow_table(self) -> Any:
        return self._answer.to_arrow_table()

    def fetch_arrow_table(self) -> Any:
        return self._answer.fetch_arrow_table()

    def fetch_record_batch(self, rows_per_batch: int = 1_000_000) -> Any:
        return self._answer.fetch_record_batch(rows_per_batch)

    def df(self) -> Any:
        return self._answer.df()


class Database:
    """THE HOST'S DATABASE, as Python's modules hold it (2026-11-12, the C checkpoint's second audit: DuckDB's own ways to
    run text or load data -- ``sql``, ``query``, ``read_csv``, ``from_arrow(...).create`` -- passed both the one send and
    the guard, one live in a planned stream step). A duckdb-python connection, or a cursor of one, behind :func:`send`
    and the few acts that are no statement: a session of its own (``cursor``), a transaction's bounds, a value attached
    by name (``register``, ``unregister``), a planned stream step's own Arrow import (``import_stream``), ``close``.
    Nothing else of DuckDB's is reachable through it, so a module holds no way to send what no plan carries. Made by
    :func:`connect`, or over a connection a person handed the host (Frames(connection=...): theirs to keep)."""

    __slots__ = ('_con',)

    def __init__(self, connection: Any) -> None:
        if isinstance(connection, (Database, Result)):
            raise TypeError('a Database holds a duckdb-python connection, not another wrapper')
        self._con = connection

    def cursor(self) -> Database:
        """A session of its own over the same database (a cursor's settings are its own)."""
        return Database(self._con.cursor())

    def begin(self) -> None:
        self._con.begin()

    def commit(self) -> None:
        self._con.commit()

    def rollback(self) -> None:
        self._con.rollback()

    def close(self) -> None:
        self._con.close()

    def register(self, name: str, value: Any) -> None:
        """A value the host holds (a frame, an Arrow table) attached by ``name``, as a plan's attach step names it."""
        self._con.register(name, value)

    def unregister(self, name: str) -> None:
        self._con.unregister(name)

    def import_stream(self, reader: Any, schema: str | None, table: str) -> None:
        """A planned stream step's landing (PlanJson ``stream``): ``reader``'s Arrow batches made into the step's table
        by the database's own Arrow import -- no statement spelled, the names the plan's, each plain or refused."""
        for name in (table, *(() if schema is None else (schema,))):
            if not _PLAIN_NAME.fullmatch(name):
                raise NotPlanned(f"a stream lands in a table named plainly (letters, digits, '_'), not {name!r}")
        self._con.from_arrow(reader).create(table if schema is None else f'{schema}.{table}')

    def _send_planned(self, sql: PlannedSql, params: Sequence[Any] | None) -> Result:
        # the one send's own check, here too (the re-audit's M2, 2026-11-12: this, `_run` until then, sent whatever it
        # was handed, and the guard refused no call of it -- it refuses any reference to it outside this module now)
        sql = require(sql, 'the database')
        return Result(self._con.execute(sql, list(params)) if params else self._con.execute(sql))


def connect(path: str | None = None) -> Database:
    """A database of the host's own (duckdb-python's, in memory unless ``path``), held as a :class:`Database`."""
    import duckdb

    return Database(duckdb.connect() if path is None else duckdb.connect(path))


def send(target: Database, statement: Any, params: Sequence[Any] | None = None) -> Result:
    """THE ONE SEND: ``statement`` run on ``target`` (the host's :class:`Database`, or a cursor of it), its ``?``s bound
    to ``params`` in order when there are any -- only if it is a plan's (:func:`require`)."""
    if not isinstance(target, Database):
        raise NotPlanned(f"the one send runs on the host's Database, not on a {type(target).__name__}")
    sql = require(statement, 'the database')
    return target._send_planned(sql, params)
