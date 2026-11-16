"""THE SERVICE KEY to the native library (//native:compiler): the run side's way in (C5-P, 2026-11-15, the orchestrator's
A1). It carries the warehouse's wire alone -- the binding that names the SQL API's calls and reads its answers, and
plans nothing (``_loader.SERVICE_KEY_NAMES``) -- and refuses every other name, so the run side reaches no planner entry
through the library: the planning key (``_library``) is no run module's to import (//python:run_closure_test). One
library, one isolate: the loader's (``_loader``), shared with the planning key."""

from __future__ import annotations

from ._loader import SERVICE_KEY_NAMES, loaded


def call(name: str, *args: str) -> str:
    """One of the warehouse's wire entries; its answer text (``OK\\n...`` or ``ERR\\n...``). Any other name is refused by
    name: the planner's, and the library's other services, are the planning key's."""
    if name not in SERVICE_KEY_NAMES:
        raise ValueError(f'{name} is not the service key\'s: it carries {sorted(SERVICE_KEY_NAMES)} alone (the planning'
                         ' key, _library.py, carries the rest)')
    return loaded().call(name, *args)
