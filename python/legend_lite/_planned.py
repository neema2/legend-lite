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
  TypeScript sends nothing else), and are checked again here at the crossing alone, ``across_the_bridge``: each must be
  a text the session's planner answered (``answered_texts``, recorded as the headless engine answers each request), or
  it is refused by name (the orchestrator's ruling of 2026-11-12). The guard (tools/guards/PlannedSqlTest.java) pins the
  crossing's callers.
- Nothing else: Python spells no SQL of its own.
"""

from __future__ import annotations

import json
from collections.abc import Container, Iterable, Sequence
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


def across_the_bridge(text: str, what: str, answered: Container[str]) -> PlannedSql:
    """A statement DataCube's hosted code hands Python's database (``what`` names the call): a PlannedSql on its side
    (planned-sql.ts), text on the bridge -- typed again here only if the session's planner answered it (``answered``:
    every text its answers carried, :func:`answered_texts`); any other text is refused by name, never sent. Its callers
    are pinned by the guard (PlannedSqlTest.BRIDGE)."""
    if not isinstance(text, str):
        raise NotPlanned(f"{what}: a statement is text, not {type(text).__name__}")
    if text not in answered:
        shown = text if len(text) <= 120 else text[:117] + '...'
        raise NotPlanned(f"{what}: not a statement this session's planner answered, so not sent: {shown!r} (a host sends "
                         'only what a plan carries or the planner wrote: _planned.py)')
    return PlannedSql(text)


def answered_texts(answer: str, into: set[str]) -> None:
    """Every text a planner's answer carries, added to ``into``: the answer's payload after its ``OK`` tag (a refusal
    carries none), an HTTP answer's body, and each string of its JSON -- a JSON text inside one read again, so a plan's
    steps, its content's statements and a host operation's statements are each one. What DataCube's hosted code may then
    hand the database (:func:`across_the_bridge`): every statement it sends is one of these, minted where it read them."""
    tag, _, payload = answer.partition('\n')
    if tag == 'OK':
        _texts(payload, into)


def _texts(text: str, into: set[str]) -> None:
    if text in into:
        return
    into.add(text)
    # an HTTP answer (pure/v1's, execute's plan half): its status, its media type, then its body
    head = text.split('\n', 2)
    if len(head) == 3 and head[0].isdigit():
        _texts(head[2], into)
    if text[:1] in ('{', '['):
        try:
            _strings(json.loads(text), into)
        except ValueError:
            pass  # a text that only looks like JSON: itself, kept above


def _strings(value: object, into: set[str]) -> None:
    if isinstance(value, str):
        _texts(value, into)
    elif isinstance(value, dict):
        for v in value.values():
            _strings(v, into)
    elif isinstance(value, list):
        for v in value:
            _strings(v, into)


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
