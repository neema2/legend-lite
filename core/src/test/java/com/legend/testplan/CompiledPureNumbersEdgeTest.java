// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.ExecuteOptions;
import com.legend.database.Databases;
import com.legend.error.AssertFailed;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;

import java.sql.Connection;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * COMPILED LEGEND-PURE'S NUMBER EQUALITY on the edge's door (register S127; census C1's K14, the orchestrator's ruling
 * of 2026-11-17: a test of upstream's behaviour is an edge test -- moved from {@code AssertBarTest}): the compatibility
 * rules, the edge's plan style and the edge's run.
 */
class CompiledPureNumbersEdgeTest {
    private static final String MODEL = """
            ###Pure
            Class t::Person { name: String[1]; }
            Class t::Employee extends t::Person {}
            function t::check(x: Integer[1]): Boolean[1] { assertEquals(2, $x) }
            function t::check2(x: Integer[1]): Boolean[1] { assertEquals(2, $x); true; }
            """;

    private static final List<com.legend.testplan.PlanningOptions.JudgeMode> JUDGES = List.of(
            com.legend.testplan.PlanningOptions.JudgeMode.HOST, com.legend.testplan.PlanningOptions.JudgeMode.DATABASE,
            com.legend.testplan.PlanningOptions.JudgeMode.BOTH);

    private static void run(Connection c, String body, ExecuteOptions options) {
        com.legend.testing.EdgeProgram.run(StorelessRuntime.with(MODEL, Databases.PLATFORM), body,
                StorelessRuntime.RUNTIME, c, options);
    }

    /** BACKWARDS-COMPATIBILITY MODE (register S127, the user's ruling of 2026-11-04: "the principled fix for the
     *  product, then compiled for backwards compatible"): compiled legend-pure's number equality -- an Integer and a
     *  Decimal equal by their texts, a Float equal to neither (CompiledSupport.eq; the engine asked:
     *  upstream-api/s127-number-equality.txt) -- under every judge alike. */
    @Test
    void backwardsCompatibilityModeComparesNumbersAsCompiledPureDoes() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                ExecuteOptions compat = com.legend.testing.EdgeProgram.compat(mode);
                run(c, "{|assertEquals(1D, 1)}", compat);
                run(c, "{|assertEquals(3, 3D)}", compat);
                for (String body : List.of("{|assertEquals(1.5, 1.5D)}", "{|assertEquals(1, 1.0)}",
                        "{|assertEquals(1.0D, 1)}")) {
                    AssertFailed e = org.junit.jupiter.api.Assertions.assertThrows(AssertFailed.class,
                            () -> run(c, body, compat), mode + " " + body);
                    assertTrue(e.getMessage().startsWith("\nexpected: "), mode + " " + body + ": " + e.getMessage());
                }
            }
        }
    }
}
