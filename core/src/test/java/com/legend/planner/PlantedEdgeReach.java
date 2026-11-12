// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * A class PLANTED in the planner's package that reaches the edge, for one test alone:
 * {@code ArchitectureTest.theEdgeRuleBitesAPlantedProductClass} shows {@code ArchitectureTest.noProductClassReachesTheEdge}
 * (G5) fails on it. A test class: the rule's real check reads the product's classes, never this.
 */
public final class PlantedEdgeReach {

    private PlantedEdgeReach() {
    }

    /** The defect the rule refuses: a product class naming an edge type. */
    public static Class<?> reach() {
        return com.legend.edge.CompatEdge.class;
    }
}
