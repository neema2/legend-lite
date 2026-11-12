"""THE ONE TEXT PYTHON'S HOST SENDS ITS DATABASE (the total wall's C3, 2026-11-12, the user's: "the hosts' separation
proven structurally"), the twin of engine-client's planned-sql.ts: a statement a PLAN carries, or one the PLANNER wrote
as its answer (a host operation's, a session's setup, a catalog's read, a project database's statements, a load's steps).

``PlannedSql`` is a ``str`` made in ONE place each, where a plan or a planner's answer is READ (``from_plan``'s callers:
compiler.py's readers, projects.py's load runners, engine.py's execute plan); ``send`` -- every statement Python's host
runs on its database goes through it -- refuses any other text by name (``NotPlanned``). A ``str`` edited after it was
read (concatenated, stripped, formatted) is a plain ``str`` again, so an edited plan statement is refused too.

Two kinds of text reach ``send`` that no reader here made, each by name:
- DataCube's own code, hosted in V8 (``_headless``): the statements its plan runner and planner client hand to Python's
  database cross the bridge as text. They are ``PlannedSql`` on that side (engine-client's planned-sql.ts: DataCube's
  TypeScript sends nothing else), and are typed again here at the crossing alone, ``across_the_bridge`` -- the guard
  (tools/guards/PlannedSqlTest.java) pins its callers.
- Nothing else: Python spells no SQL of its own.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any


class PlannedSql(str):
    """A statement a plan carries or the planner wrote: the only text ``send`` runs."""

    __slots__ = ()


class NotPlanned(TypeError):
    """A text sent to a database that no plan carries: refused, never run."""


def from_plan(text: str) -> PlannedSql:
    """``text`` as the plan or the planner's answer it was READ from carries it: called by the plan readers alone (the
    guard, tools/guards/PlannedSqlTest.java, holds it so). The text is never edited, only typed."""
    if not isinstance(text, str):
        raise NotPlanned(f"a plan's statement is text, not {type(text).__name__}")
    return text if isinstance(text, PlannedSql) else PlannedSql(text)


def all_from_plan(texts: Iterable[str]) -> list[PlannedSql]:
    """Each of ``texts``, as :func:`from_plan` types one."""
    return [from_plan(t) for t in texts]


def across_the_bridge(text: str, what: str) -> PlannedSql:
    """A statement DataCube's hosted code hands Python's database (``what`` names the call): a PlannedSql on its side
    (planned-sql.ts), text on the bridge, typed again here. Its callers are pinned by the guard
    (PlannedSqlTest.BRIDGE)."""
    if not isinstance(text, str):
        raise NotPlanned(f"{what}: a statement is text, not {type(text).__name__}")
    return PlannedSql(text)


def require(statement: Any, where: str) -> PlannedSql:
    """``statement`` if it is a plan's; else refused by name, ``where`` saying what it was about to be sent to."""
    if isinstance(statement, PlannedSql):
        return statement
    shown = statement if isinstance(statement, str) else type(statement).__name__
    if isinstance(shown, str) and len(shown) > 120:
        shown = shown[:117] + '...'
    raise NotPlanned(f"not a plan's statement, so not sent to {where}: {shown!r} (a host sends only what a plan "
                     'carries or the planner wrote: _planned.py)')


def send(target: Any, statement: Any, params: Sequence[Any] | None = None) -> Any:
    """THE ONE SEND: ``statement`` run on ``target`` (a duckdb-python connection or cursor), its ``?``s bound to
    ``params`` in order when there are any -- only if it is a plan's (:func:`require`)."""
    sql = require(statement, 'the database')
    return target.execute(sql, list(params)) if params else target.execute(sql)
