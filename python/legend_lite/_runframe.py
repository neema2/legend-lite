"""THE RUN FRAME (the next PR's item 2, "separation made foolproof": the standing witness, 2026-11-13; Java's is
RunScope, the tab's engine-client's run-frame.ts). While a plan's statements run, the flow that runs them asks the
planner nothing: plan only plans, exec only execs (G4). A run is marked around the steps that send a plan's statements
(``in_run``, ``runs``); the native library's one entry asks first (``planning``), and a call made inside a run of its
own flow is refused by name.

The frame is the FLOW's, not the process's: a context variable, so each thread (an engine server's pool answering two
requests at once) and each asyncio task has its own, and one request planning while another's plan runs is no
crossing."""

from __future__ import annotations

import contextlib
import contextvars
import functools
from collections.abc import Callable, Iterator
from typing import Any, TypeVar

_RUN: contextvars.ContextVar[str | None] = contextvars.ContextVar('legend_lite_run', default=None)

F = TypeVar('F', bound=Callable[..., Any])


@contextlib.contextmanager
def in_run(what: str) -> Iterator[None]:
    """A run that ``what`` names, for the ``with`` block: every planner entry it reaches, on its own flow, refused."""
    token = _RUN.set(what)
    try:
        yield
    finally:
        _RUN.reset(token)


def runs(what: str) -> Callable[[F], F]:
    """A function whose every call is a run that ``what`` names."""
    def wrap(f: F) -> F:
        @functools.wraps(f)
        def run(*args: Any, **kwargs: Any) -> Any:
            with in_run(what):
                return f(*args, **kwargs)
        return run  # type: ignore[return-value]
    return wrap


# THE KNOWN CROSSINGS, each a planner call a run is known to make, named until it is fixed (shrink-only; each entry
# dated and reported): the witness steps out of the run for it alone (``exempt``), and refuses any exemption not listed.
EXEMPTIONS = frozenset({
    # found by the witness on 2026-11-13 (test_engine.PlanAndRunRoutes.test_sql_the_database_refuses_is_answered_with_
    # its_plan): a read whose statement fails re-pins its cursor, and the pin's statement is asked of the planner then
    # (frames.Reading.execute -> _pin -> _host.count -> compiler.host_operations); the fix -- the pin planned when the
    # read begins -- awaits the orchestrator's word
    "a read's re-pin after its statement failed",
})


@contextlib.contextmanager
def exempt(why: str) -> Iterator[None]:
    """The ``with`` block outside any run, for a known crossing ``why`` names (one of EXEMPTIONS, or refused)."""
    if why not in EXEMPTIONS:
        raise ValueError(f'no known crossing is named {why!r}: a run plans nothing (G4)')
    token = _RUN.set(None)
    try:
        yield
    finally:
        _RUN.reset(token)


def current_run() -> str | None:
    """The run this flow is inside, by its name; None outside any."""
    return _RUN.get()


def planning(kind: str) -> None:
    """A planner entry, asked for ``kind``: refused inside a run of its own flow."""
    run = _RUN.get()
    if run is not None:
        raise RuntimeError(f"the planner was asked {kind!r} inside a run ({run}): a run sends its plan's statements and"
                           ' plans nothing (G4)')
