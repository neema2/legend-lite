// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning.planted.edge;

import com.legend.tools.meaning.planted.PlantedLowering;

/** PLANTED (census B2): the edge's routes, and its live use of the product's prune. */
public final class PlantedEdgeRoutes {

    public String mount() {
        return "/api/server/v1/currentUser";
    }

    public static String answer(String query) {
        return PlantedLowering.prune(query, "unionAlias_" + query.length());
    }
}
