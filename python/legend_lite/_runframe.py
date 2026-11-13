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
    """A function whose every call is a run that ``what`` names.

    The variable is set and reset here directly, not through ``in_run``'s generator: the same frame at half the cost per
    run (measured 2026-11-13, docs/witness-cost-homework: around a real DuckDB statement the generator's enter and exit
    cost ~5.6 us a run, a direct set and reset ~2 us)."""
    def wrap(f: F) -> F:
        @functools.wraps(f)
        def run(*args: Any, **kwargs: Any) -> Any:
            token = _RUN.set(what)
            try:
                return f(*args, **kwargs)
            finally:
                _RUN.reset(token)
        return run  # type: ignore[return-value]
    return wrap


# THE KNOWN CROSSINGS, each a planner call a run is known to make, named until it is fixed (shrink-only; each entry
# dated and reported): the witness steps out of the run for it alone (``exempt``), and refuses any exemption not listed.
# Empty since 2026-11-13: its one entry, "a read's re-pin after its statement failed" (found by the witness that day:
# frames.Reading.execute re-pinned a read whose statement failed, the pin's statement asked of the planner inside the
# read's run), went with the user's ruling (1b): a read ends at its first failure as a query runs, re-pinned never.
EXEMPTIONS: frozenset[str] = frozenset()


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
