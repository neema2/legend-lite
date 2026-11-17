"""LEGEND-ENGINE COMPATIBILITY, AT THE EDGE (the edge program's H5 groundwork, 2026-11-17): the edge's own door composes
an engine with the edge's routes (``legend_lite.edge.engine``), as Java's EdgeServer composes the server -- the one
place the two meet. This target alone depends on //python:edge; the product's tests run without it."""

import unittest

import legend_lite as ll
from legend_lite import edge
from legend_lite._edge_seam import NO_EDGE_ROUTES, EdgeRoutes


class TheEdgesDoor(unittest.TestCase):

    def test_the_door_composes_an_engine_with_the_edges_routes(self):
        engine = edge.engine(ll.Frames())
        self.assertIsInstance(engine.edge, edge.EngineRoutes)
        self.assertIsInstance(engine.edge, EdgeRoutes)
        self.assertIsNot(engine.edge, NO_EDGE_ROUTES)
        # nothing moved yet (the groundwork): a call no route serves is refused as the product refuses it
        said = engine.answer('POST', '/no-such-route.json', '', '{}')
        self.assertEqual((said.status, said.body), (404, b'no such path: /no-such-route.json'))

    def test_the_door_gives_the_routes_itself(self):
        with self.assertRaisesRegex(TypeError, "the edge's door gives the engine its routes itself"):
            edge.engine(ll.Frames(), edge=NO_EDGE_ROUTES)


if __name__ == '__main__':
    unittest.main()
