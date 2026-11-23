"""THE EDGE'S SEAM IN THE PRODUCT (the edge program's H5 groundwork, 2026-11-17, the orchestrator's ruling D-a): how
Python's engine is given the routes of another API than its own -- legend-engine's, which live in ``legend_lite.edge``,
the one package that composes them (its own door, ``legend_lite.edge.engine``, as Java's EdgeServer composes the
server). The product never imports the edge: it names only this Protocol and its own refusing object, ``NoEdgeRoutes``,
which every engine carries unless the edge's door gives it another -- never ``None``, never a default that answers
(G7). The dependency runs one way, edge -> product (//python:run_closure_test's layer rule; no product library depends
on //python:edge)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .engine import Answer, Engine


@runtime_checkable
class EdgeRoutes(Protocol):
    """The routes an engine answers besides its own: ``serves`` says which calls are theirs (by method and path: the
    engine, and its HTTP server's admission, ask it), ``answer`` answers one, with the engine that asks (``engine``) --
    its frames, its stores -- and the call as it came."""

    def serves(self, method: str, path: str) -> bool: ...

    def answer(self, engine: Engine, method: str, path: str, query: str, body: str | None) -> Answer: ...


class NoEdgeRoutes:
    """THE PRODUCT'S OWN: no other API is served. A call the engine's own routes do not serve is refused by name, in the
    product's words (the sentence the engine has always answered such a call with; it follows lite's API spec when the
    user approves it)."""

    def serves(self, method: str, path: str) -> bool:
        return False

    def answer(self, engine: Any, method: str, path: str, query: str, body: str | None) -> Answer:
        # the engine's own answer type, read where it is asked (the engine module imports this one)
        from .engine import Answer

        return Answer(404, "text/plain; charset=utf-8", f"no such path: {path}".encode("utf-8"))


# the one product object every engine carries unless the edge's door gives it its routes
NO_EDGE_ROUTES = NoEdgeRoutes()


def edge_routes(routes: Any) -> EdgeRoutes:
    """``routes`` when it is an ``EdgeRoutes``; anything else -- ``None`` among them -- refused by name: an engine always
    answers an unknown call through one, the product's own when no edge gave it another."""
    if not isinstance(routes, EdgeRoutes):
        raise TypeError(
            f"an engine's edge routes answer what its own routes do not (EdgeRoutes): not {routes!r} --"
            " the product's are NO_EDGE_ROUTES, the edge's are composed by legend_lite.edge.engine"
        )
    return routes
