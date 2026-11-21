// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

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

    /** The compatibility edge's options: the plan handle ({@code executionPlan}'s value, its nodes and text) is
     *  legend-engine's, so a body that reads one runs on the edge's style (D's hook fold, a4ac096b5, 2026-11-16). */
    private static final ExecuteOptions EDGE = ExecuteOptions.NONE.withStyle(com.legend.edge.PureText.UPSTREAM);

    /** The judges a body on the edge runs under: the host's alone (rule 4 of the compatibility program's rules of
     *  record; A4 of the compatibility program, stage A ratified by the user 2026-11-19: until A4 every judge, the
     *  database half's planning of these shapes on the edge the point; the product's database half keeps it, below). */
    private static final PlanningOptions.JudgeMode[] EDGE_JUDGES = {PlanningOptions.JudgeMode.HOST};

    private static String run(String model, String body, PlanningOptions.JudgeMode mode) {
        return run(model, body, mode, ExecuteOptions.NONE);
    }

    private static String onTheEdge(String model, String body, PlanningOptions.JudgeMode mode) {
        return run(model, body, mode, EDGE);
    }

    private static String run(String model, String body, PlanningOptions.JudgeMode mode, ExecuteOptions options) {
        try (Connection conn = TestBackends.platform().open()) {
            var ctx = Compiler.compileModel(StorelessRuntime.with(model, TestBackends.platform().type()));
            com.legend.TestExecution.runProgram(Compiler.query(ctx, body), StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(conn)), null,
                    // the edge's style is heard by the edge's run hook (census C1, 2026-11-17: one choice of words)
                    options == EDGE ? com.legend.edge.AppealRun.with(null) : com.legend.testexec.NoEdgeRun.NONE,
                    options.withJudgeMode(mode));
            return "PASS";
        } catch (Exception | AssertionError e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    @Test
    void aDualGoldenRootIsJudgedByTheDatabaseHalf() {
        String passing = "{| meta::relational::functions::sqlQueryToString::h2::assertEqualsH2Compatible('a', 'b', 'b');}";
        String failing = "{| meta::relational::functions::sqlQueryToString::h2::assertEqualsH2Compatible('a', 'b', 'c');}";
        for (PlanningOptions.JudgeMode mode : EDGE_JUDGES) {
            // a dual golden is upstream's engine-text root: the edge's facet plans it (W9's judge half, S2b)
            assertEquals("PASS", onTheEdge(DUAL, passing, mode), mode + ": the upgraded golden equals ours");
            String failed = onTheEdge(DUAL, failing, mode);
            assertTrue(failed.startsWith("AssertFailed"), mode + ": the upgraded golden differs: " + failed);
        }
    }

    /** A let's mapping error -- the corpus's plan-handle idiom (testModelConnectionJoin's `let res = executionPlan(...)`)
     *  over a class the mapping does not map -- refuses the body at planning under every judge, nothing run, an assert
     *  before it reporting nothing (the user's option 1 of 2026-11-21, register S393: a mapping error is refused at
     *  planning until its codes land, docs/records/compat-mapping-errors-followup-2026-11-21.md; until then the error
     *  was held for the next assert's turn, the PR1 re-review's B1). */
    @Test
    void aLetsMappingErrorRefusesTheBodyAtPlanningUnderEveryJudge() {
        String let = " let p = meta::pure::executionPlan::executionPlan(|p::B.all()->project([b|$b.x], ['x']),"
                + " p::M, " + StorelessRuntime.RUNTIME + ".runtimeValue, []);"
                + " assertNotEmpty($p.rootExecutionNode.executionNodes);}";
        for (String before : List.of("{|", "{| assertEquals(1, 2);", "{| assertEquals(1, 1);")) {
            String body = before + let;
            String host = onTheEdge(UNMAPPED, body, PlanningOptions.JudgeMode.HOST);
            assertTrue(host.startsWith("MappingResolutionException"), "HOST, " + before + ": " + host);
            for (PlanningOptions.JudgeMode mode : EDGE_JUDGES) {
                assertEquals(host, onTheEdge(UNMAPPED, body, mode), mode + ", " + before + ": the host's own refusal");
            }
        }
    }

    /** A let's error upstream raises AT RUN (a code stating RUNNING: timeBucket's StrictDate unit, legend-engine's
     *  TimeBucketShared) is held for the next assert's turn under every judge: an assert before it runs and reports
     *  first, and with a passing one before it the let's error is the body's at that turn (the PR1 re-review's B1;
     *  the BE audit 2's S6, 2026-11-21: option 1 took the mapping case these tests had used). */
    @Test
    void aLetsRunTimeErrorIsHeldForTheNextAssertUnderEveryJudge() {
        String let = " let d = %2024-01-31->meta::pure::functions::date::timeBucket(1,"
                + " meta::pure::functions::date::DurationUnit.HOURS); assertEquals(%2024-01-31, $d);}";
        for (PlanningOptions.JudgeMode mode : PlanningOptions.JudgeMode.values()) {
            String failedFirst = run("", "{| assertEquals(1, 2);" + let, mode);
            assertTrue(failedFirst.startsWith("AssertFailed"), mode + ": the assert before the let reports first: "
                    + failedFirst);
            String raised = run("", "{| assertEquals(1, 1);" + let, mode);
            assertTrue(raised.startsWith("ModelException") && raised.contains("Unsupported duration unit for StrictDate"),
                    mode + ": the let's error at the next assert's turn: " + raised);
        }
    }

    /** The product's style plans no plan handle: {@code executionPlan}'s value is legend-engine's, the compatibility
     *  edge's alone, so on the product a body that binds one is refused BY NAME at that let, under every judge (D's
     *  hook fold, 2026-11-16: "executionPlan with no edge is refused by name"). */
    @Test
    void aPlanHandleOnTheProductIsRefusedByNameUnderEveryJudge() {
        String body = "{| let p = meta::pure::executionPlan::executionPlan(|p::A.all()->project([a|$a.x], ['x']),"
                + " p::M, " + StorelessRuntime.RUNTIME + ".runtimeValue, []);"
                + " assertNotEmpty($p.rootExecutionNode.executionNodes);}";
        for (PlanningOptions.JudgeMode mode : PlanningOptions.JudgeMode.values()) {
            String got = run(UNMAPPED, body, mode);
            assertTrue(got.startsWith("NotImplementedException") && got.contains("compatibility edge"),
                    mode + ": " + got);
        }
    }
}
