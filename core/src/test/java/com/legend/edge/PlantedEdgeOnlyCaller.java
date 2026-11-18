// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.PlantedEdgeOnlyApi;

/** Planted for {@code EdgeOnlyMembersTest} (J40 of the edge audit, 2026-11-18): the edge's uses of a planted product
 *  class. Read as bytecode alone; never called by a test. */
@SuppressWarnings("unused")
public final class PlantedEdgeOnlyCaller {

    private PlantedEdgeOnlyCaller() {
    }

    static Object uses() {
        PlantedEdgeOnlyApi a = new PlantedEdgeOnlyApi("x");
        return java.util.List.of(PlantedEdgeOnlyApi.edgeOnly(), PlantedEdgeOnlyApi.shared(), a.edgeField, a.toString(),
                PlantedEdgeOnlyApi.EDGE_CONSTANT, new PlantedEdgeOnlyApi.Hooked().hooked(),
                PlantedEdgeOnlyApi.Hooked.also(), PlantedEdgeOnlyApi.countdown(3), PlantedEdgeOnlyApi.Kind.values(),
                PlantedEdgeOnlyApi.viaDead(), PlantedEdgeOnlyApi.viaMain(),
                PlantedEdgeOnlyApi.viaPublicDead());
    }
}
