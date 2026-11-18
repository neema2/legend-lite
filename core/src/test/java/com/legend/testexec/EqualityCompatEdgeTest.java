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
 * 2026-11-17: a test of upstream's behaviour is an edge test, judged through the edge's door -- the run hook's
 * {@code HostForms} and the plan's rules -- never by calling the host judge's equality itself).
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
    void compatibilityModeEqualsAnIntegerAndADecimalByTheirTexts() throws Exception {
        // register S127: compiled legend-pure's CompiledSupport.eq -- 1D == 1 by text, a Float equal to neither
        try (Connection c = TestBackends.platform().open()) {
            holds(c, "|assertEquals(1D, 1)");
            fails(c, "|assertEquals(1.0, 1)");
        }
    }

    @Test
    void compatibilityModeHoldsTwoUlpsAndTheEngineSeamsScaleSensitiveEquals() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // registers S20/S132: the 2-ULP tolerance is compatibility mode's alone
            holds(c, "|assertEquals(0.3, 0.1 + 0.2)");
            // register S139 (X2, VERDICT_RULE_AUDIT: engine Decimal equality is getValue().equals): scale-sensitive
            fails(c, "|assertEquals(3.0D, 3.00D)");
            fails(c, "|assertEquals(1.50D, 1.5D)");
        }
    }
}
