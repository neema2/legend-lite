// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A USER'S FAIL IS NEVER A SITE (phase D's Phase 2, raises as SITES, 2026-11-16; the D part 1 audit's S4): a site's
 * token opens with the kind mark ({@code RaiseKind.TOKEN}), so a user's {@code fail('LG#1')} -- the token's digits
 * spelled without it -- comes back as the user's own message on the platform database, never read as the statement's site 1,
 * even where the statement declares one.
 */
class UserFailIsNeverASiteTest {

    @Test
    void aFailSpelledAsATokenIsTheUsersMessageOnThePlatformDatabase() throws Exception {
        var backend = TestBackends.platform();
        // a statement with a site of its own (the division's guard) beside the user's fail
        String query = "|#TDS\n  x:Integer, y:Integer\n  4, 2\n#->extend(~q: r|$r.x / $r.y)->extend(~r: r|fail('LG#1'))"
                + "->select(~[q, r])->from(" + StorelessRuntime.RUNTIME + ")";
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(backend.withStorelessRuntime("")), query)
                .executionPlan(StorelessRuntime.RUNTIME);
        assertTrue(!plan.answerSql().sites().isEmpty(), "the statement declares a site: " + plan.answerSql());
        StringWriter out = new StringWriter();
        com.legend.error.DataError raised = assertThrows(com.legend.error.DataError.class, () -> {
            try (java.sql.Connection c = backend.open()) {
                PlanRunner.stream(plan, Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                        PlanCases.Written.JSON.writer(out, plan.answerShape()), java.util.UUID.randomUUID());
            }
        });
        assertNull(raised.kind(), "the user's message, not a site: " + raised.getMessage());
        assertEquals("LG#1", raised.getMessage());
    }
}
