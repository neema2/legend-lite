// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.ExecuteOptions;
import com.legend.database.Databases;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Program;

import java.sql.Connection;

import org.junit.jupiter.api.Test;

/**
 * AN ASSERT WHOSE SIDES BOTH RAISE FAILS WITH THE FIRST SIDE'S ERROR, EVERY TIME (G10, the orchestrator's MUST of
 * 2026-11-17, from F's update_edge_contract_22: testAdjustByDaysBigNumber's DuckDB error flipped between its two
 * sides'): the host plan's side batch reads its sides in order ({@code HostSideBatch}), as upstream evaluates an
 * assert's arguments left to right. Run many times: the host judge, and isolated BOTH under
 * the compatibility rules (PCT's), whose answer is the host's error.
 */
class HostSideOrderTest {

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";
    /** The expected side's date is out of DuckDB's range; the actual side's day count overflows its INT32. */
    private static final String BODY = "|assertEquals(%33803336-12-17, %2014-02-27->adjust(12345678912,"
            + " meta::pure::functions::date::DurationUnit.DAYS))";

    @Test
    void theFirstSidesErrorEveryTime() throws Exception {
        for (int run = 0; run < 25; run++) {
            try (Connection c = TestBackends.platform().open()) {
                RuntimeException e = assertThrows(RuntimeException.class, () -> Program.run(
                        StorelessRuntime.with(MODEL, Databases.PLATFORM), BODY, StorelessRuntime.RUNTIME, c,
                        ExecuteOptions.NONE.withJudgeMode(PlanningOptions.JudgeMode.HOST)));
                assertFirstSides("HOST run " + run, e);
            }
        }
    }

    /** F's case as PCT runs it: the host judge alone under the compatibility rules on the edge's door (A4 of the
     *  compatibility program, stage A ratified by the user 2026-11-19: until A4 isolated BOTH), the host's error the
     *  body's answer. */
    @Test
    void theFirstSidesErrorUnderTheCompatibilityRules() throws Exception {
        for (int run = 0; run < 25; run++) {
            try (Connection c = TestBackends.platform().open()) {
                RuntimeException e = assertThrows(RuntimeException.class, () -> com.legend.testing.EdgeProgram.run(
                        StorelessRuntime.with(MODEL, Databases.PLATFORM), BODY, StorelessRuntime.RUNTIME, c,
                        com.legend.testing.EdgeProgram.compat(PlanningOptions.JudgeMode.HOST)));
                assertFirstSides("compat HOST run " + run, e);
            }
        }
    }

    private static void assertFirstSides(String what, RuntimeException e) {
        String m = String.valueOf(e.getMessage());
        assertTrue(m.contains("out of range") && !m.contains("INT32"), what + ": " + m);
    }
}
