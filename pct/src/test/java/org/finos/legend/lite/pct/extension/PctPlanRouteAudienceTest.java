// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Channel A's one audience a run (PR #127's review, 2026-11-19: #124's ENGINE lane and the LITE-PCT lane chose on the
 *  same lines of PctPlanRoute.execute): each property alone names its audience, and the two together, or any other
 *  value, are refused by name. */
class PctPlanRouteAudienceTest {

    @Test
    void eachPropertyAloneNamesItsAudience() {
        assertEquals(PctPlanRoute.Audience.LEGEND_PURE, PctPlanRoute.audience(null, "COMPAT"));
        assertEquals(PctPlanRoute.Audience.LEGEND_ENGINE, PctPlanRoute.audience(PctPlanRoute.ENGINE_AUDIENCE, "COMPAT"));
        assertEquals(PctPlanRoute.Audience.LITE, PctPlanRoute.audience(null, "LITE"));
    }

    @Test
    void legendEnginesAudienceUnderLitesRulesIsRefusedByName() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PctPlanRoute.audience(PctPlanRoute.ENGINE_AUDIENCE, "LITE"));
        assertTrue(e.getMessage().contains("-Dpct.audience=engine with -Dpct.channela.rules=LITE"), e.getMessage());
    }

    @Test
    void anyOtherValueIsRefusedByName() {
        assertTrue(assertThrows(IllegalArgumentException.class, () -> PctPlanRoute.audience("pure", "COMPAT"))
                .getMessage().contains("-Dpct.audience=pure"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> PctPlanRoute.audience(null, "lite"))
                .getMessage().contains("-Dpct.channela.rules=lite"));
    }
}
