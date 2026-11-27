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
 * door judges numbers, Decimals and Floats by lite's rules, as lite's own runs do (the Float's 2-ULP tolerance, S15,
 * went with step 1b the same day: legend-engine compares exactly).
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
        // register S127: numbers are of two families -- an exact number (an Integer, a Decimal) never equals a Float,
        // two exact numbers are equal by their value (the user's ruling of 2026-11-27) -- on the edge's door too, by
        // the one rulebook of B3 step 1a (2026-11-24). For 1D == 1 that is compiled legend-pure's verdict again
        // (CompiledSupport.eq, by text), which the edge held until B3 and lite's rule denied from then until this
        // ruling
        try (Connection c = TestBackends.platform().open()) {
            holds(c, "|assertEquals(1D, 1)");
            fails(c, "|assertEquals(1.5D, 1)");
            fails(c, "|assertEquals(1.0, 1)");
            fails(c, "|assertEquals(1.5, 1.5D)");
        }
    }

    @Test
    void theEdgesDoorComparesFloatsExactlyAndDecimalsByValue() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // register S15: a Float compared exactly on the edge's door too since B3 step 1b (2026-11-24; until then
            // compiled legend-pure's 2-ULP tolerance there). A Float literal is the double on every door since the
            // user's ruling of 2026-11-25 ("yes delete the float decimal thing"; register S20, measured by R1's M1):
            // 0.1 + 0.2 is 0.30000000000000004, compiled Pure's answer (legend-engine on DuckDB adds two DECIMAL
            // literals to 0.3, the one recorded difference); two Float values one ULP apart are not equal
            holds(c, "|assertEquals(0.30000000000000004, 0.1 + 0.2)");
            fails(c, "|assertEquals(0.3, 0.1 + 0.2)");
            fails(c, "|assertEquals(0.30000000000000004, 0.3)");   // one ULP apart: the old tolerance held it
            // register S139: a Decimal by value, its scale not part of it -- on the edge's door too since B3 step 1a
            // (2026-11-24; until then compiled legend-pure's scale-sensitive equals there)
            holds(c, "|assertEquals(3.0D, 3.00D)");
            holds(c, "|assertEquals(1.50D, 1.5D)");
            fails(c, "|assertEquals(1.5D, 1.51D)");
        }
    }
}
