// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * LEGEND-ENGINE'S API AS AN OPT-IN MODULE OF THE SHIPPED SERVER (K2's server half, the user's ruling of 2026-11-18:
 * "Product root, edge opt-in"): registered in {@code META-INF/services/com.legend.server.MountProvider}, found by the
 * product's server only when its configuration says {@code engine: legend-engine} ({@code --engine legend-engine}),
 * and then mounting {@link EdgeServer#UPSTREAM}: pure/v1's routes and
 * legend-engine's query store.
 */
public final class EngineMount implements com.legend.server.MountProvider {

    /** The configuration's name of legend-engine's module. */
    public static final String ENGINE = "legend-engine";

    @Override
    public String engine() {
        return ENGINE;
    }

    @Override
    public com.legend.server.LegendHttpServer.Mount mount() {
        return EdgeServer.UPSTREAM;
    }
}
