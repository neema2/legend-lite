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

import threading
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass, field
from typing import Any

from . import compiler

# the plan the planner answered, by the type the runners take: named here as it always was (C5-P)
from ._planned import PlanText  # noqa: F401

# the runners, the run side's since C5-P (2026-11-15: _runs.py): named here as they always were
from ._runs import (
    PLAN_VERSION,  # noqa: F401 -- read from this module by others
    HostLoad,  # noqa: F401 -- read from this module by others
    Landed,  # noqa: F401 -- read from this module by others
    LoadRefused,  # noqa: F401 -- read from this module by others
    open_project,
    run_host_load,  # noqa: F401
    run_load,  # noqa: F401 -- read from this module by others
    run_write,  # noqa: F401 -- read from this module by others
)


class NotAModel(compiler.LegendError):
    """The compiler's refusal of a project's model -- it does not parse, or does not compile -- as ``using`` raises it
    when asked to surface it (its message and kind the compiler's)."""


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


# how many model texts' database names a ProjectDatabases remembers (ProjectDatabases._named): a page's few projects
_NAMED_KEPT = 8


class ProjectDatabases:
    """The project databases on one DuckDB connection (an engine's Frames'): ``using`` a model's, made the first time."""

    def __init__(
        self,
        connection: Any,
        database_type: str = "DuckDB",
        guard: Callable[[], AbstractContextManager[Any]] = nullcontext,
    ) -> None:
        """``guard``: what every use of ``connection`` from another thread holds (an engine's Frames' lock): a database
        is made under it, never beside a query on the frames."""
        self._connection = connection
        self._guard = guard
        self._type = database_type
        self._open: dict[str, _Open] = {}
        # each opened project's model text -> its database's name, the latest _NAMED_KEPT texts (2026-11-19,
        # //python:markets_test's time: every use asked the native library for the 5 MB markets model's statements
        # again, 0.5 s a time, 40 times in that test, only to find the name of the database already open -- the
        # statements are the text's and the database type's alone, compiler.project_data). Keyed by the whole text, so
        # an edited text is asked afresh; a text let go is only asked again
        self._named: OrderedDict[str, str] = OrderedDict()
        self._lock = threading.RLock()

    @property
    def names(self) -> list[str]:
        """The project databases this process holds."""
        with self._lock:
            return list(self._open)

    @contextmanager
    def using(self, model: str, *, surface: bool = False) -> Iterator[Project | None]:
        """``model``'s project database (named by what it holds) -- a cursor on it and the model held -- made and filled
        the first time, this thread's alone until the block ends; None when the model has no Data rows, or does not
        read as a model (refused where a model is) -- or, ``surface``, the compiler's own refusal of it raised
        (``NotAModel``: a saved query's plan says why its model is no model, never that it has no rows; the C5
        follow-up's held item, 2026-11-15)."""
        with self._lock:
            named = self._named.get(model)
            if named is not None:
                self._named.move_to_end(model)
            opened = None if named is None else self._open.get(named)
        if opened is not None:
            with opened.lock:
                yield Project(opened.cursor, opened.held)
            return
        try:
            statements = compiler.project_data(model, self._type)
        except compiler.LegendError as refused:
            # a model that does not read as a model (it does not parse or compile, by the product's own kinds:
            # compiler.refuses_the_text) is no project's; any other refusal (a Data element filling a table no Database
            # declares) is said as it is
            if not compiler.refuses_the_text(refused):
                raise
            if surface:
                raise NotAModel(refused.message, refused.kind, refused.where, refused.raise_) from refused
            statements = None
        if statements is None or statements.get("load") is None:
            yield None
            return
        with self._guard(), self._lock:
            opened = self._open.get(statements["name"])
            if opened is None:
                opened = self._make(model, statements)
                self._open[statements["name"]] = opened
            self._named[model] = statements["name"]
            self._named.move_to_end(model)
            while len(self._named) > _NAMED_KEPT:
                self._named.popitem(last=False)
        with opened.lock:
            yield Project(opened.cursor, opened.held)

    def _make(self, model: str, statements: dict[str, Any]) -> _Open:
        """The planner's part first -- the model held, the session's setup -- then the database made and filled by the
        run side (``_runs.open_project``); one that fails lets its model go."""
        held = compiler.HeldModel(model)
        try:
            setup = compiler.session_setup(self._type)
            cursor = open_project(self._connection, statements, setup)
        except Exception:
            held.close()
            raise
        return _Open(cursor, held)
