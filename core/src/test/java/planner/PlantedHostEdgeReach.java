// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planner;

/**
 * A class PLANTED in a host's package (the boundary's, {@code planner}) that names an edge type, for one test alone (the
 * GUARD stream's C.5 review, 2026-11-17): {@code ArchitectureTest.theWidenedRulesBiteAPlantedHostDoor} shows the edge
 * rule over the hosts' doors fails on it. In the edge's test library (core/edge_tests.bzl), the one that may name an
 * edge type.
 */
public final class PlantedHostEdgeReach {

    private PlantedHostEdgeReach() {
    }

    public static Class<?> reach() {
        // an edge type the host may not name: PureText since 2026-11-19 (A2 of the compatibility program; it named
        // EngineTextDialect, deleted with legend-engine's SQL text)
        return com.legend.edge.PureText.class;
    }
}
