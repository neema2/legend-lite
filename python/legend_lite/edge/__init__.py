"""LEGEND-ENGINE COMPATIBILITY, AT THE EDGE (the edge program's H5, its groundwork 2026-11-17; the orchestrator's ruling
D-a): the one Python package that serves legend-engine's API -- the product answers in lite's own forms, and a
legend-engine client is served here, on top of it. The dependency runs ONE way: this package imports the product,
and no product module imports it (//python:run_closure_test's layer rule; no product library depends on
//python:edge), as Java's com.legend.edge depends on the product and never the reverse.

Its door composes an engine with the edge's routes (``engine``), as Java's EdgeServer composes the server: the product's
engine carries its own refusal (``_edge_seam.NO_EDGE_ROUTES``) unless this door gives it ``EngineRoutes``.

The groundwork moves no route: ``EngineRoutes`` answers nothing of its own yet, and refuses as the product does.
legend-engine's routes -- pure/v1's grammar, compilation and execute (JSON and Arrow) and its saved-query store at
``/api/pure/v1/query`` -- move here from ``legend_lite.engine`` last (H5), once the TS hosts' clients ask lite's own."""

from __future__ import annotations

from typing import Any

from .._edge_seam import NO_EDGE_ROUTES
from ..engine import Answer, Engine
from ..frames import Frames


class EngineRoutes:
    """LEGEND-ENGINE'S ROUTES, served by an engine composed at this door: none moved yet (the groundwork) -- each call
    refused as the product refuses one (``NO_EDGE_ROUTES``)."""

    def answer(self, engine: Engine, method: str, path: str, query: str, body: str | None) -> Answer:
        return NO_EDGE_ROUTES.answer(engine, method, path, query, body)


def engine(frames: Frames, **kwargs: Any) -> Engine:
    """An engine over ``frames`` that serves legend-engine's API beside lite's own (``EngineRoutes``): the edge's door,
    the one place the two are composed. ``kwargs``: the engine's own (``site``, ``headless``, ``pages``, ``keeper``)."""
    if 'edge' in kwargs:
        raise TypeError("the edge's door gives the engine its routes itself: pass no 'edge'")
    return Engine(frames, edge=EngineRoutes(), **kwargs)
