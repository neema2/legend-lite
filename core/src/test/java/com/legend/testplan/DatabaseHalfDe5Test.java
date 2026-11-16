// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DE5'S SECOND GROUP (2026-11-16; pulled forward for PR1 by the orchestrator under the user's delegation: the five corpus
 * bodies the deleted second road had planned under BOTH): the database judge's half plans what the host's plans.
 * <ul>
 * <li>The dual-golden root ({@code assertEqualsH2Compatible}) whose text arm declines is gated as its own meaning on our
 * H2, {@code assertEquals(upgraded, actual)}, as the host plans it ({@code AssertVerdicts.gateOf}); the gate had looked
 * for a verdict native on the helper's own name and refused the body.</li>
 * <li>A let whose model error raises at planning (a class its mapping does not map) fails the body with that error
 * under every judge, as the host's planner fails it; the database half had refused the body for it.</li>
 * </ul>
 */
class DatabaseHalfDe5Test {

    private static final String DUAL = """
            function meta::relational::functions::sqlQueryToString::h2::assertEqualsH2Compatible(legacyExpected: Any[1],
                upgradedExpected: Any[1], actual: Any[1]): Boolean[1]
            {
              assertEquals($upgradedExpected, $actual);
            }
            """;

    private static final String UNMAPPED = """
            ###Pure
            Class p::A { x: Integer[1]; }
            Class p::B { x: Integer[1]; }
            ###Mapping
            Mapping p::M
            (
              p::A: Pure
              {
                ~src p::A
                x: $src.x
              }
            )
            """;

    private static String run(String model, String body, PlanningOptions.JudgeMode mode) {
        try (Connection conn = TestBackends.platform().open()) {
            var ctx = Compiler.compileModel(StorelessRuntime.with(model, TestBackends.platform().type()));
            com.legend.TestExecution.runProgram(Compiler.query(ctx, body), StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(conn)), null, null,
                    ExecuteOptions.NONE.withJudgeMode(mode));
            return "PASS";
        } catch (Exception | AssertionError e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    @Test
    void aDualGoldenRootIsJudgedByTheDatabaseHalf() {
        String passing = "{| meta::relational::functions::sqlQueryToString::h2::assertEqualsH2Compatible('a', 'b', 'b');}";
        String failing = "{| meta::relational::functions::sqlQueryToString::h2::assertEqualsH2Compatible('a', 'b', 'c');}";
        for (PlanningOptions.JudgeMode mode : PlanningOptions.JudgeMode.values()) {
            assertEquals("PASS", run(DUAL, passing, mode), mode + ": the upgraded golden equals ours");
            String failed = run(DUAL, failing, mode);
            assertTrue(failed.startsWith("AssertFailed"), mode + ": the upgraded golden differs: " + failed);
        }
    }

    @Test
    void aLetsModelErrorFailsTheBodyUnderEveryJudge() {
        // the corpus's plan-handle idiom (testModelConnectionJoin's `let res = executionPlan(...)`), over a class the
        // mapping does not map: the let raises its model error as it is planned
        String body = "{| let p = meta::pure::executionPlan::executionPlan(|p::B.all()->project([b|$b.x], ['x']),"
                + " p::M, " + StorelessRuntime.RUNTIME + ".runtimeValue, []);"
                + " assertNotEmpty($p.rootExecutionNode.executionNodes);}";
        String host = run(UNMAPPED, body, PlanningOptions.JudgeMode.HOST);
        for (PlanningOptions.JudgeMode mode : PlanningOptions.JudgeMode.values()) {
            String got = run(UNMAPPED, body, mode);
            assertTrue(got.startsWith("MappingResolutionException"), mode + ": " + got);
            assertEquals(host, got, mode + ": the host's own error");
        }
    }

    /** The PR1 re-review's B1 (G2): an assert BEFORE the let runs and reports first, under every judge as under the
     *  host: the let's model error is held for the next assert's turn, never raised at planning ahead of it. */
    @Test
    void anAssertBeforeALetsModelErrorReportsFirstUnderEveryJudge() {
        String body = "{| assertEquals(1, 2); let p = meta::pure::executionPlan::executionPlan(|p::B.all()->project("
                + "[b|$b.x], ['x']), p::M, " + StorelessRuntime.RUNTIME + ".runtimeValue, []);"
                + " assertNotEmpty($p.rootExecutionNode.executionNodes);}";
        String host = run(UNMAPPED, body, PlanningOptions.JudgeMode.HOST);
        assertTrue(host.startsWith("AssertFailed"), "HOST: " + host);
        for (PlanningOptions.JudgeMode mode : PlanningOptions.JudgeMode.values()) {
            assertEquals(host, run(UNMAPPED, body, mode), mode + ": the earlier assert's failure first");
        }
    }

    /** B1's other half: a passing assert before the let, then the let's model error at the next assert's turn. */
    @Test
    void aLetsModelErrorAfterAPassingAssertRaisesAtTheNextAssertUnderEveryJudge() {
        String body = "{| assertEquals(1, 1); let p = meta::pure::executionPlan::executionPlan(|p::B.all()->project("
                + "[b|$b.x], ['x']), p::M, " + StorelessRuntime.RUNTIME + ".runtimeValue, []);"
                + " assertNotEmpty($p.rootExecutionNode.executionNodes);}";
        String host = run(UNMAPPED, body, PlanningOptions.JudgeMode.HOST);
        assertTrue(host.startsWith("MappingResolutionException"), "HOST: " + host);
        for (PlanningOptions.JudgeMode mode : PlanningOptions.JudgeMode.values()) {
            assertEquals(host, run(UNMAPPED, body, mode), mode.toString());
        }
    }
}
