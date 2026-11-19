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

    /** The edge's judges: the host's alone (rule 4 of the compatibility program's rules of record; A4 of the
     *  compatibility program, stage A ratified by the user 2026-11-19: until A4 the host's, the database's and both). */
    private static final List<com.legend.testplan.PlanningOptions.JudgeMode> JUDGES = List.of(
            com.legend.testplan.PlanningOptions.JudgeMode.HOST);

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

    /** A Decimal property over an INTEGER column (PR #118 fix round, 2026-11-18, G2; AssertBarTest's product half): on
     *  the edge, compiled legend-pure's rule decides it -- the Integer 1 and the property's Decimal 1 equal by their texts
     *  -- under every judge alike: one verdict per assert (assertEq and assertIs hold, assertNotEq and assertIsNot fail).
     *  The failures' legend-pure words still differ by judge on this shape (the value's %r: the host's "1" for the map's
     *  integer carrier, the database's "1DD" for the property's D-suffixed carrier): reported open, not pinned here. */
    private static final String MAPPED = """
            ###Pure
            Class p::P { id: Integer[1]; d: Decimal[1]; }
            ###Relational
            Database p::DB ( Table T (ID INTEGER PRIMARY KEY, D INTEGER) )
            ###Mapping
            Mapping p::M ( p::P: Relational { ~mainTable [p::DB] T id: T.ID, d: T.D } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,D\\n1,1\\n'; }; auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """.formatted(Databases.PLATFORM);

    @Test
    void anEqOverADeclaredDecimalIsOneVerdictUnderEveryJudgeOnTheEdge() throws Exception {
        java.util.List<String> wrong = new java.util.ArrayList<>();
        for (String side : List.of("p::P.all()->filter(x | $x.id == 1)->toOne().d", "p::P.all()->map(x | $x.d)->toOne()")) {
            for (String fn : List.of("assertEq", "assertIs", "assertNotEq", "assertIsNot")) {
                String body = "{|" + fn + "(1, " + side + ")}";
                String want = fn.endsWith("Not") || fn.endsWith("NotEq") ? "AssertFailed" : "pass";
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        com.legend.testing.EdgeProgram.runFresh(MAPPED, body, "p::RT",
                                com.legend.testing.EdgeProgram.compat(mode));
                        got = "pass";
                    } catch (Throwable e) {
                        got = e.getClass().getSimpleName();
                    }
                    if (!want.equals(got)) {
                        wrong.add(mode + " " + body + " -> " + got);
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one verdict under every judge:\n  " + String.join("\n  ", wrong));
    }
}
