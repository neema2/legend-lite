"""THE EDGE'S KEY TO THE ENGINE LIBRARY (K2, the user's "product root, edge opt-in", 2026-11-18): legend-engine's entry
points -- pure/v1, execute's halves, the refusal in legend-engine's envelope, its saved-query store -- live in a native
library of their own (//native:engine, libcompiler_engine, shipped by the legend-lite-engine wheel: the [engine]
extra), loaded here on first use, its own isolate. The product's planning key (_library) and service key (_services)
reach the product library alone, which exports none of these; only this package asks the engine library. Without it
installed, legend-engine's routes are refused by name ("legend-engine needs the legend-lite[engine] extra")."""

from __future__ import annotations

from pathlib import Path

from .._loader import EngineMissing, engine_loaded

__all__ = ["EngineMissing", "engine_library"]


class EngineLibrary:
    """The engine library as the edge asks it: one entry point, its answer text, the native string freed (the
    loader's ``call``: legend-engine's planning entries refused inside a run, as the product's are)."""

    @property
    def path(self) -> Path:
        return engine_loaded().path

    def call(self, name: str, *args: str) -> str:
        return engine_loaded().call(name, *args)

    def exports(self, name: str) -> bool:
        """Whether the engine library exports a function of that name: its own symbol table asked."""
        return engine_loaded().exports(name)

    def unfreed(self) -> int:
        return engine_loaded().unfreed()


_key = EngineLibrary()


def engine_library() -> EngineLibrary:
    """The process's engine library, through the edge's key (loaded on first use; refused by name, ``EngineMissing``,
    where the [engine] extra is not installed)."""
    engine_loaded()
    return _key
