"""THE PLANNING KEY to the native library (//native:compiler): the planner's way in, and every Python caller's but the
run side's (C5-P, 2026-11-15, the orchestrator's A1: until then this module was the library's one way in, calling any
name; the loader is ``_loader`` now, which only the two keys import). It carries every entry but the warehouse's wire
(``_loader.SERVICE_KEY_NAMES``, the service key's: ``_services``), each as before: a planner's entry refused inside a
run of its flow (``_runframe.planning``), the library's services and a held model let go answered anywhere
(``_NOT_PLANNING``)."""

from __future__ import annotations

from pathlib import Path

from ._loader import NOT_PLANNING, SERVICE_KEY_NAMES, loaded

# The library's entries that plan nothing: its services (the site's types, the warehouse's binding, the model home's
# stores) and a held model let go -- which a finalizer may do at any point, a run's included. Every other entry is the
# planner's, refused inside a run of its flow (_runframe.py, the witness, 2026-11-13).
# (The warehouse's two are the service key's since C5-P, refused by this key before this set is read; they stay
# listed, as the native services PlannerSurfaceTest holds this set to.)
_NOT_PLANNING = frozenset({
    'lite_site_types', 'lite_warehouse_call', 'lite_warehouse_read', 'lite_sdlc_handle', 'lite_depot_handle',
    'lite_saved_queries', 'lite_saved_cubes', 'lite_release_model',
})
if _NOT_PLANNING != NOT_PLANNING:
    raise ImportError(f'the planning key\'s services ({sorted(_NOT_PLANNING)}) are not the loader\'s'
                      f' ({sorted(NOT_PLANNING)}): one set, written twice for PlannerSurfaceTest, held equal')


class Library:
    """The planning key: the native library as the planner and the doors ask it."""

    @property
    def path(self) -> Path:
        return loaded().path

    def call(self, name: str, *args: str) -> str:
        """One entry point; its answer text (``OK\\n...`` or ``ERR\\n...``), the native string freed. A planner's entry
        is refused inside a run of its flow (``_runframe.planning``); the warehouse's wire is the service key's, refused
        here by name."""
        if name in SERVICE_KEY_NAMES:
            raise ValueError(f"{name} is the warehouse's wire, the run side's service key's (_services.py), not the"
                             " planning key's")
        # a planner's entry refused inside a run by the loader itself (C5-P's audit M1): no path past it
        return loaded().call(name, *args)

    def exports(self, name: str) -> bool:
        """Whether the library exports a function of that name: its own symbol table asked, so no list is kept by hand."""
        return loaded().exports(name)

    def unfreed(self) -> int:
        """The answers the library has returned that were not yet freed (its own count): 0 between calls."""
        return loaded().unfreed()


_key = Library()


def library() -> Library:
    """The process's one library, through the planning key (loaded on first use, here, as before the split)."""
    loaded()
    return _key
