// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.error.AssertFailed;
import com.legend.error.NotImplementedException;
import com.legend.exec.StatementOrigin;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import com.legend.testexec.TestRunner;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.TestPlanJson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BOTH JUDGES OVER THE DATABASE JUDGE'S REAL PROGRAM (E-5c step 6, the user's ruling O5 of 2026-11-13):
 * {@link AgreementAsserts#planFused} plans a body as the database judge's segments of fused verdict groups, the host
 * judge reading each single-value side as typed columns of the fused row. Each body is planned on the in-process
 * platform database and RUN through the real test runner: two scalar asserts become one fused group of two roots with
 * fused-cell sides and two agreements; a failing assert fails by name in the host's words; a grid's sides keep their
 * own statements; an assertError keeps its own steps; a root whose rows ride no fused statement is refused by name
 * (pending the ruling Q-E5c-1). The database judge's own plan is the one it was: no host column, no host verdict.
 */
class AgreementFusedPlanTest {

    private static final String TWO_SCALARS = "{| assertEquals(2, 1 + 1); assertEquals('ab', 'a' + 'b');}";
    private static final String SECOND_FAILS = "{| assertEquals(2, 1 + 1); assertEquals(3, 1 + 1);}";
    private static final String A_GRID = "{| assertEquals(2, 1 + 1);"
            + " assertEquals(#TDS\na\n2\n#, #TDS\na\n1\n2\n#->filter(r|$r.a > 1));}";
    private static final String LAMBDAS = "{| assertEquals(1, [1, 2, 3]->filter(x|$x > 2)->size());"
            + " assertEquals(1, 1 + 0);}";
    private static final String AN_ASSERT_ERROR = "{| assertEquals(2, 1 + 1); assertError(|assertEquals(1, 2), 'x');}";

    private static Connection conn;
    private static ModelContext ctx;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
        ctx = Compiler.compileModel(StorelessRuntime.with("", Databases.PLATFORM));
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    @DisplayName("two scalar asserts of one segment: ONE fused group of two roots, fused-cell sides, two agreements;"
            + " both pass")
    void twoScalarAssertsRideOneFusedGroup() {
        TestPlan plan = fused(TWO_SCALARS);
        List<TestPlan.Verdicts> groups = groups(plan);
        assertEquals(1, groups.size(), "one segment, one fused group");
        TestPlan.Verdicts g = groups.get(0);
        assertNotNull(g.fused(), "the database judge's fused statement");
        assertNotNull(g.roots(), "the segment's root loop");
        assertEquals(2, g.roots().roots().size(), "two roots");
        assertEquals(2, g.agreements().size(), "one agreement per root");
        for (int k = 0; k < 2; k++) {
            assertEquals(k, g.agreements().get(k).root());
            int carrier = g.agreements().get(k).hostVerdict();
            TestPlan.Verdict v = g.verdicts().stream().filter(x -> x.id() == carrier).findFirst().orElseThrow();
            assertNotNull(v.host(), "the carrier holds the host's facts");
            assertEquals(2, v.base().size());
            v.base().forEach(side -> assertInstanceOf(TestPlan.Side.FusedCell.class, side,
                    "a single-value side read from the fused row"));
        }
        List<String> fusedColumns = g.fused().columns().stream().map(ExecutionPlan.Column::name).toList();
        assertTrue(fusedColumns.containsAll(List.of("__h0_0_0", "__h0_1_0", "__h1_0_0", "__h1_1_0")),
                fusedColumns.toString());
        Told told = new Told();
        run(plan, told);
        assertEquals(List.of("assertEquals pass", "assertEquals pass"), told.verdicts);
    }

    @Test
    @DisplayName("a failing assert fails by name in the host's words, after the one before it passed")
    void aFailingAssertFailsInTheHostsWords() {
        TestPlan plan = fused(SECOND_FAILS);
        Told told = new Told();
        AssertFailed failed = assertThrows(AssertFailed.class, () -> run(plan, told));
        assertFalse(failed.getMessage().contains("DISAGREE"), failed.getMessage());
        assertTrue(failed.getMessage().contains("expected: 3"), failed.getMessage());
        assertEquals(List.of("assertEquals pass", "assertEquals fail: " + failed.getMessage()), told.verdicts);
    }

    @Test
    @DisplayName("an assert over a grid keeps its own side statements; the scalar one beside it is read from the row")
    void aGridKeepsItsOwnSideStatements() {
        TestPlan plan = fused(A_GRID);
        List<TestPlan.Verdicts> groups = groups(plan);
        assertEquals(1, groups.size());
        TestPlan.Verdicts g = groups.get(0);
        TestPlan.Verdict scalar = carrier(g, 0);
        TestPlan.Verdict grid = carrier(g, 1);
        scalar.base().forEach(side -> assertInstanceOf(TestPlan.Side.FusedCell.class, side));
        grid.base().forEach(side -> assertInstanceOf(TestPlan.Side.Rows.class, side, "a grid's own statement"));
        Told told = new Told();
        run(plan, told);
        assertEquals(List.of("assertEquals pass", "assertEquals pass"), told.verdicts);
    }

    @Test
    @DisplayName("a side computed over lambdas is a column of the row too, and holds")
    void aSideOverLambdasIsAColumn() {
        TestPlan plan = fused(LAMBDAS);
        TestPlan.Verdicts g = groups(plan).get(0);
        carrier(g, 0).base().forEach(side -> assertInstanceOf(TestPlan.Side.FusedCell.class, side));
        Told told = new Told();
        run(plan, told);
        assertEquals(List.of("assertEquals pass", "assertEquals pass"), told.verdicts);
    }

    @Test
    @DisplayName("an assertError keeps its own steps, as the agreement judge plans them today; the segment before it"
            + " closes, and the run tells what today's plan tells")
    void anAssertErrorKeepsItsOwnSteps() {
        TestPlan plan = fused(AN_ASSERT_ERROR);
        List<TestPlan.Verdicts> groups = groups(plan);
        assertEquals(3, groups.size(), "the segment's group, the host's step of the inner assert, the raising one");
        assertTrue(groups.get(2).agreements().get(0).raising(), "the raising verdict's agreement");
        // the message 'x' is not the inner assert's: it fails, under both plans alike
        Told fusedTold = new Told();
        AssertFailed fusedFailed = assertThrows(AssertFailed.class, () -> run(plan, fusedTold));
        Told todayTold = new Told();
        AssertFailed todayFailed = assertThrows(AssertFailed.class, () -> run(today(AN_ASSERT_ERROR), todayTold));
        assertEquals(todayFailed.getMessage(), fusedFailed.getMessage());
        assertEquals(todayTold.verdicts, fusedTold.verdicts);
        assertEquals("assertEquals pass", fusedTold.verdicts.get(0));
    }

    @Test
    @DisplayName("an assert under an if: each side's asserts the database judge's segments with the host's half"
            + " attached; the side taken runs and holds")
    void aBranchsSidesAreFusedToo() {
        TestPlan plan = fused("{| assertEquals(2, 1 + 1);"
                + " if(1 < 2, |assertEquals(2, 1 + 1), |assertEquals(3, 1 + 1));}");
        TestPlan.Branch branch = plan.steps().stream().filter(TestPlan.Branch.class::isInstance)
                .map(TestPlan.Branch.class::cast).findFirst().orElseThrow();
        for (List<TestPlan.TestStep> side : List.of(branch.then(), branch.otherwise())) {
            assertEquals(1, side.size());
            TestPlan.Verdicts g = (TestPlan.Verdicts) side.get(0);
            assertEquals(1, g.agreements().size());
            carrier(g, 0).base().forEach(s -> assertInstanceOf(TestPlan.Side.FusedCell.class, s));
        }
        Told told = new Told();
        run(plan, told);
        assertEquals(List.of("assertEquals pass", "assertEquals pass"), told.verdicts);
    }

    @Test
    @DisplayName("a root whose rows ride no fused statement (decided at planning) is refused by name, pending Q-E5c-1")
    void anUncarriedRootIsRefusedByName() {
        NotImplementedException refused = assertThrows(NotImplementedException.class,
                () -> fused("{| assertIs(Integer, Integer);}"));
        assertEquals("meta::pure::functions::asserts::assertIs: an agreement root whose rows ride no fused statement"
                + " (E-5c, pending the ruling Q-E5c-1)", refused.getMessage());
    }

    @Test
    @DisplayName("the database judge's plan is its own: no host column, no host verdict, the same groups as the fused"
            + " plan's, its fused statement the same columns but the host's")
    void theDatabaseJudgesPlanIsUnchanged() {
        for (String body : List.of(TWO_SCALARS, SECOND_FAILS, A_GRID, LAMBDAS, AN_ASSERT_ERROR)) {
            TestPlan database = planned(body, PlanningOptions.JudgeMode.DATABASE);
            String json = TestPlanJson.write(database);
            assertFalse(java.util.regex.Pattern.compile("__h\\d").matcher(json).find(),
                    body + ": a host column in the database judge's plan");
            assertFalse(json.contains("\"fusedCell\""), body + ": a fused-cell side in the database judge's plan");
            assertEquals(json, TestPlanJson.write(planned(body, PlanningOptions.JudgeMode.DATABASE)),
                    body + ": the database judge's plan, planned twice");
            if (body.equals(AN_ASSERT_ERROR)) {
                continue;
            }
            TestPlan.Verdicts db = groups(database).get(0);
            TestPlan.Verdicts both = groups(fused(body)).get(0);
            assertEquals(db.verdicts().size(), both.verdicts().size(), body);
            List<String> dbColumns = db.fused().columns().stream().map(ExecutionPlan.Column::name).toList();
            List<String> bothColumns = both.fused().columns().stream().map(ExecutionPlan.Column::name)
                    .filter(n -> !n.matches("__h\\d+_\\d+_\\d+")).toList();
            assertEquals(dbColumns, bothColumns, body);
            for (TestPlan.Verdict v : db.verdicts()) {
                assertNull(v.host(), body + ": a host verdict in the database judge's plan");
            }
        }
    }

    // ---- helpers ----

    private static TestPlan fused(String body) {
        TypedQuery typed = typed(body);
        ExecuteOptions options = ExecuteOptions.NONE.withJudgeMode(PlanningOptions.JudgeMode.BOTH);
        BodyDriver.ExecEnv env = BodyDriver.bodyEnv(typed, StorelessRuntime.RUNTIME,
                Databases.dialect(Databases.PLATFORM), options.planning());
        TestDoor.Asserts asserts = AgreementAsserts.planFused(typed, StorelessRuntime.RUNTIME, env, null, false);
        assertNotNull(asserts, body + ": planned for both judges");
        TestPlan plan = TestDoor.planned(typed, StorelessRuntime.RUNTIME, options.planning().features(), asserts);
        assertEquals(TestPlan.Judge.AGREEMENT, plan.judge());
        assertNull(TestPlanJson.roundTripDifference(plan), "the plan reads back as written");
        return plan;
    }

    /** {@code body} planned for both judges as the test planner plans it today ({@link AgreementAsserts#plan}). */
    private static TestPlan today(String body) {
        TypedQuery typed = typed(body);
        ExecuteOptions options = ExecuteOptions.NONE.withJudgeMode(PlanningOptions.JudgeMode.BOTH);
        BodyDriver.ExecEnv env = BodyDriver.bodyEnv(typed, StorelessRuntime.RUNTIME,
                Databases.dialect(Databases.PLATFORM), options.planning());
        AgreementAsserts asserts = AgreementAsserts.plan(typed, StorelessRuntime.RUNTIME, env, null, false);
        assertNotNull(asserts, body + ": planned for both judges today");
        return TestDoor.planned(typed, StorelessRuntime.RUNTIME, options.planning().features(), asserts);
    }

    private static TestPlan planned(String body, PlanningOptions.JudgeMode mode) {
        TypedQuery typed = typed(body);
        ExecuteOptions options = ExecuteOptions.NONE.withJudgeMode(mode);
        BodyDriver.ExecEnv env = BodyDriver.bodyEnv(typed, StorelessRuntime.RUNTIME,
                Databases.dialect(Databases.PLATFORM), options.planning());
        DatabaseAsserts asserts = DatabaseAsserts.plan(typed, StorelessRuntime.RUNTIME, env);
        assertNotNull(asserts, body + ": planned for the database judge");
        return TestDoor.planned(typed, StorelessRuntime.RUNTIME, options.planning().features(), asserts);
    }

    private static List<TestPlan.Verdicts> groups(TestPlan plan) {
        List<TestPlan.Verdicts> out = new ArrayList<>();
        for (TestPlan.TestStep step : plan.steps()) {
            if (step instanceof TestPlan.Verdicts g) {
                out.add(g);
            }
        }
        return out;
    }

    /** Root {@code k}'s carrier verdict in its segment's closing group {@code g}. */
    private static TestPlan.Verdict carrier(TestPlan.Verdicts g, int k) {
        int id = g.agreements().get(k).hostVerdict();
        return g.verdicts().stream().filter(v -> v.id() == id).findFirst().orElseThrow();
    }

    /** The verdicts the listener was told, in order: the assert's arm, then "pass" or "fail: " and its message. */
    private static final class Told implements AssertListener {
        final List<String> verdicts = new ArrayList<>();

        @Override
        public void verdict(String assertName, boolean pass, String message) {
            String arm = assertName.substring(assertName.lastIndexOf(':') + 1);
            verdicts.add(arm + (pass ? " pass" : " fail: " + message));
        }
    }

    private static void run(TestPlan plan, Told told) {
        TestRunner.run(plan, TestSessions.over(conn), com.legend.executionplan.DataStores.none(), null, told, null,
                null, StatementOrigin.OTHER, UUID.randomUUID());
    }

    private static TypedQuery typed(String body) {
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(body);
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : List.of(parsed);
        return Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(List.of()), ctx));
    }
}
