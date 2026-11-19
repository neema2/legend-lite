// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The audience channel A planned each case as, counted in this JVM, as the route itself chose it
 * ({@code PctPlanRoute.execute}: the edge's text and the feature flags it handed the run). The ENGINE lane checks it
 * after its last case ({@code Test_LegendLite_Engine_PCT.AudienceCheck}; H7's independent review, 2026-11-19): a route
 * that stopped reading {@code -Dpct.audience} would plan every case as the PCT's own audience, and the lane would still
 * pass every case lite passes there, quietly.
 */
public final class RouteAudience {

    private static final AtomicLong AS_LEGEND_ENGINE = new AtomicLong();

    private static final AtomicLong OTHERWISE = new AtomicLong();

    private RouteAudience() {
    }

    /** One case planned: as legend-engine's audience (the pure/v1 route's text, no feature flag), or otherwise. */
    static void planned(boolean asLegendEngine) {
        (asLegendEngine ? AS_LEGEND_ENGINE : OTHERWISE).incrementAndGet();
    }

    /** The cases planned as legend-engine's audience in this JVM. */
    public static long plannedAsLegendEngine() {
        return AS_LEGEND_ENGINE.get();
    }

    /** The cases planned as any other audience in this JVM. */
    public static long plannedOtherwise() {
        return OTHERWISE.get();
    }
}
