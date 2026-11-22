// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.database.PlanStyle;
import com.legend.edge.PureText;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A GRAPH FETCH'S KEY ARGUMENTS IN THE PLAN'S STYLE (phase D's D1.2, 2026-11-15; register S312, the orchestrator under
 * the user's ruling of 2026-11-10): the product spells each literal argument as its type's product text -- a DateTime
 * ISO 8601 in UTC with six fraction digits and {@code Z}, a date as written, a String itself -- and the compatibility
 * edge as legend-engine does ({@code +0000} after a time, a String quoted).
 */
class GraphKeyTextTest {

    @Test
    void theProductSpellsEachArgumentAsItsTypesText() {
        assertEquals("2023-10-15T00:00:00.000000000Z", PlanStyle.PRODUCT.graphKeyDate("2023-10-15T00:00:00"));
        assertEquals("2023-10-15T10:30:00.000000000Z", PlanStyle.PRODUCT.graphKeyDate("2023-10-15T10:30"));
        assertEquals("2023-10-15T10:00:00.000000000Z", PlanStyle.PRODUCT.graphKeyDate("2023-10-15T10"));
        assertEquals("2023-10-15T10:30:01.500000000Z", PlanStyle.PRODUCT.graphKeyDate("2023-10-15T10:30:01.5"));
        assertEquals("2023-10-15T10:30:01.123456789Z", PlanStyle.PRODUCT.graphKeyDate("2023-10-15T10:30:01.123456789"));
        assertEquals("2023-10-15", PlanStyle.PRODUCT.graphKeyDate("2023-10-15"));
        assertEquals("abc", PlanStyle.PRODUCT.graphKeyString("abc"));
    }

    @Test
    void theEdgeSpellsEachArgumentAsLegendEngineDoes() {
        assertEquals("2023-10-15T00:00:00+0000", PureText.UPSTREAM.graphKeyDate("2023-10-15T00:00:00"));
        assertEquals("2023-10-15", PureText.UPSTREAM.graphKeyDate("2023-10-15"));
        assertEquals("'abc'", PureText.UPSTREAM.graphKeyString("abc"));
    }
}
