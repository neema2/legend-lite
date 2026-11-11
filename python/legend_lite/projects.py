"""A PROJECT'S OWN DATABASE on Python's DuckDB (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5), the twin of the tab's
(engine-client/src/project-data.ts): where a project version's Data rows live when Query or Studio, served by Python,
runs one of its queries, or a saved query opens on Python's page -- a database of their own inside Python's DuckDB, its
schemas as the model declares them, never beside a person's frames. What it is, and every statement that makes and
points at it, is the native library's (``compiler.project_data``, the platform's ProjectData in Java, by this
database's type), named by what it holds (the model's Databases and Data rows); Python spells none. Its rows are the
plan's: the ONCE load a server's project plan carries (``load``), run here (``run_load``), its statements a blob checked
against its digest (B3, 2026-11-11: until then the fill's statements as text, run outside any plan, census M14). Made
the first time a query over the model asks, on a cursor of its own, and kept for the process's life (the user,
2026-10-13: no strategy for letting one go; legend-lite's server keeps its own so too). A model with no Data rows has
none."""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass, field
from typing import Any

from . import compiler

# the kinds the compiler names a model's failure to read as a model by (planner.Folded, PureV1Api.refusal): such a
# model is no project's, and is refused where a model is; any other refusal of a project's database (a Data element
# filling a table no Database declares) is said as it is
_NOT_A_MODEL = frozenset({'PARSER', 'COMPILATION'})


class LoadRefused(ValueError):
    """A plan's load this host does not run, whose data is not the data it was planned against, or whose statement the
    database refused (its words, the data named): refused by name."""


def run_load(plan: Any, execute: Callable[[str], Any]) -> None:
    """A plan whose root is the ONCE load a project's database is made with (the native library's ``load``: the platform's
    LoadPlanner.projectData, the load a server's project plan carries), run by ``execute`` on the database Python made
    for it: its steps in order -- a statement as it is; a content blob of statements, its bytes checked against the
    digest and size its reference names, each statement as written, a refusal naming its data; a file's load refused by
    name (no store a host's plan names). The rule is engine-client's plan-runner.ts ``runLoad`` (the tab's), the same plan
    format (B3, 2026-11-11)."""
    root = plan.get('root') if isinstance(plan, dict) else None
    if plan.get('format') != 'legend-lite-plan' or not isinstance(root, dict) or root.get('_type') != 'load':
        raise LoadRefused('a load\'s plan is a legend-lite plan with a load at its root')
    if root.get('cadence') != 'ONCE':
        raise LoadRefused(f"a load's plan made with a database is ONCE, not {root.get('cadence')}")
    content = root.get('content') or {}
    for step in root['steps']:
        kind = step.get('_type')
        if kind == 'statement':
            execute(step['sql'])
        elif kind == 'statements':
            ref, of = step['ref'], step['of']
            text = content.get(ref['digest'])
            if text is None:
                raise LoadRefused(f"the load {root['identity']} reads the content {ref['digest']}, which its plan does not carry")
            data = text.encode('utf-8')
            digest = hashlib.sha256(data).hexdigest()
            if digest != ref['digest'] or len(data) != ref['size']:
                raise LoadRefused(f'{of} is not the data it was planned against: {len(data)} bytes of digest {digest}, '
                                  f"where the plan names {ref['size']} bytes of digest {ref['digest']}")
            statements = json.loads(text)
            if not isinstance(statements, list) or not statements or not all(isinstance(x, str) for x in statements):
                raise LoadRefused(f'{of}: a statements blob is a non-empty JSON array of strings')
            for sql in statements:
                try:
                    execute(sql)
                except Exception as refused:
                    # the database's own words, the data named (as the tab's runner says them)
                    raise LoadRefused(f'{of}: {refused}') from refused
        elif kind == 'file':
            raise LoadRefused(f"the file '{step['ref']['name']}' is a load this host does not run: a host's files are no "
                              'store a plan names yet')
        else:
            raise LoadRefused(f"a load's step is a statement, a statements blob or a file, not a {kind}")


@dataclass(frozen=True)
class Project:
    """A project's database in use: a cursor pointed at it, and its model held."""

    cursor: Any
    held: compiler.HeldModel


@dataclass
class _Open:
    cursor: Any
    # the project's model, held: what its queries are planned against (a page's planner requests, plan-and-run.json)
    held: compiler.HeldModel
    # one use at a time: a DuckDB connection is not safe from several threads at once (the audit of C5)
    lock: threading.Lock = field(default_factory=threading.Lock)


class ProjectDatabases:
    """The project databases on one DuckDB connection (an engine's Frames'): ``using`` a model's, made the first time."""

    def __init__(self, connection: Any, database_type: str = 'DuckDB',
                 guard: Callable[[], AbstractContextManager[Any]] = nullcontext) -> None:
        """``guard``: what every use of ``connection`` from another thread holds (an engine's Frames' lock): a database
        is made under it, never beside a query on the frames."""
        self._connection = connection
        self._guard = guard
        self._type = database_type
        self._open: dict[str, _Open] = {}
        self._lock = threading.RLock()

    @property
    def names(self) -> list[str]:
        """The project databases this process holds."""
        with self._lock:
            return list(self._open)

    @contextmanager
    def using(self, model: str) -> Iterator[Project | None]:
        """``model``'s project database (named by what it holds) -- a cursor on it and the model held -- made and filled
        the first time, this thread's alone until the block ends; None when the model has no Data rows, or does not
        read as a model (refused where a model is)."""
        try:
            statements = compiler.project_data(model, self._type)
        except compiler.LegendError as refused:
            if refused.kind not in _NOT_A_MODEL:
                raise
            statements = None
        if statements is None or statements.get('load') is None:
            yield None
            return
        with self._guard(), self._lock:
            opened = self._open.get(statements['name'])
            if opened is None:
                opened = self._make(model, statements)
                self._open[statements['name']] = opened
        with opened.lock:
            yield Project(opened.cursor, opened.held)

    def _make(self, model: str, statements: dict[str, Any]) -> _Open:
        held = compiler.HeldModel(model)
        cursor = None
        try:
            for sql in statements['open']:
                self._connection.execute(sql)
            cursor = self._connection.cursor()
            # a cursor's session is its own: set up as every connection Python plans for is (frames.py)
            for sql in [*compiler.session_setup(self._type), *statements['use']]:
                cursor.execute(sql)
            # its rows: the plan's load (B3)
            run_load(statements['load'], cursor.execute)
        except Exception:
            # a half-made database left behind would be found again by its name: drop it, and let its model go
            if cursor is not None:
                cursor.close()
            for sql in statements['close']:
                self._connection.execute(sql)
            held.close()
            raise
        return _Open(cursor, held)
