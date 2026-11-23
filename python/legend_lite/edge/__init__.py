"""LEGEND-ENGINE COMPATIBILITY, AT THE EDGE (the edge program's H5, 2026-11-17; its groundwork the same day, the
orchestrator's ruling D-a): the one Python package that serves legend-engine's API -- the product answers in lite's own
forms, and a legend-engine client is served here, on top of it. The dependency runs ONE way: this package imports the
product, and no product module imports it (//python:run_closure_test's layer rule; no product library depends on
//python:edge), as Java's com.legend.edge depends on the product and never the reverse.

Its door composes an engine with the edge's routes (``engine``), as Java's EdgeServer composes the server: the product's
engine carries its own refusal (``_edge_seam.NO_EDGE_ROUTES``) unless this door gives it ``EngineRoutes`` --
legend-engine's ``pure/v1`` API, its execute (Arrow and JSON) and its saved-query store at ``/api/pure/v1/query``
(``_pure_v1``), moved here from ``legend_lite.engine`` once no product client asked them -- answered by the ENGINE
library (``_engine``: //native:engine, the legend-lite-engine wheel, the [engine] extra; K2, 2026-11-18), which this
door requires: without it, refused by name."""

from __future__ import annotations

from typing import Any

from ..engine import Engine
from ..frames import Frames
from ._engine import EngineMissing, engine_library
from ._pure_v1 import EngineRoutes

__all__ = ["EngineMissing", "EngineRoutes", "engine"]


def engine(frames: Frames, **kwargs: Any) -> Engine:
    """An engine over ``frames`` that serves legend-engine's API beside lite's own (``EngineRoutes``): the edge's door,
    the one place the two are composed. ``kwargs``: the engine's own (``site``, ``headless``, ``pages``, ``keeper``)."""
    if "edge" in kwargs:
        raise TypeError("the edge's door gives the engine its routes itself: pass no 'edge'")
    # legend-engine's routes answer from the engine library: none installed, refused here, by name, before any route is
    # served (EngineMissing: "legend-engine needs the legend-lite[engine] extra"), never a route that fails later
    engine_library()
    return Engine(frames, edge=EngineRoutes(), **kwargs)
