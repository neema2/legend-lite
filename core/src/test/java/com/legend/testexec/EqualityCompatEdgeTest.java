// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testexec;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.database.Databases;
import com.legend.error.AssertFailed;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.EdgeProgram;
import com.legend.testplan.PlanningOptions;

import java.sql.Connection;

import org.junit.jupiter.api.Test;

/**
 * THE COMPATIBILITY RULES' HOST EQUALITY, an edge test (census C1's K14 and step 2c, the orchestrator's rulings of
 * 2026-11-17: a test of upstream's behaviour is an edge test, judged through the edge's door, never by calling the host
 * judge's equality itself). Since B3 step 1a of the compatibility program (2026-11-24: one rulebook, lite's), the edge's
 * door judges numbers and Decimals by lite's rules, as lite's own runs do; only the Float's 2-ULP tolerance (S15) is
 * still the edge's, until step 1b places its tests against legend-engine's own answers.
 */
class EqualityCompatEdgeTest {

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";

    private static void holds(Connection c, String body) {
        assertTrue(EdgeProgram.run(StorelessRuntime.with(MODEL, Databases.PLATFORM), body, StorelessRuntime.RUNTIME,
                c, EdgeProgram.compat(PlanningOptions.JudgeMode.HOST)).passed(), body);
    }

    private static void fails(Connection c, String body) {
        assertThrows(AssertFailed.class, () -> EdgeProgram.run(StorelessRuntime.with(MODEL, Databases.PLATFORM), body,
                StorelessRuntime.RUNTIME, c, EdgeProgram.compat(PlanningOptions.JudgeMode.HOST)), body);
    }

    @Test
    void theEdgesDoorComparesNumberKindsByLitesRule() throws Exception {
        // register S127: two numbers of different kinds are never equal -- on the edge's door too since B3 step 1a
        // (2026-11-24; until then compiled legend-pure's CompiledSupport.eq there, 1D == 1 by text)
        try (Connection c = TestBackends.platform().open()) {
            fails(c, "|assertEquals(1D, 1)");
            fails(c, "|assertEquals(1.0, 1)");
        }
    }

    @Test
    void theEdgesDoorHoldsTwoUlpsAndComparesDecimalsByValue() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // registers S15/S132: the 2-ULP tolerance is still the edge's, until B3 step 1b
            holds(c, "|assertEquals(0.3, 0.1 + 0.2)");
            // register S139: a Decimal by value, its scale not part of it -- on the edge's door too since B3 step 1a
            // (2026-11-24; until then compiled legend-pure's scale-sensitive equals there)
            holds(c, "|assertEquals(3.0D, 3.00D)");
            holds(c, "|assertEquals(1.50D, 1.5D)");
            fails(c, "|assertEquals(1.5D, 1.51D)");
        }
    }
}
