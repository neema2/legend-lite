// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.error.AssertFailed;
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
import java.util.Map;
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
        assertEquals("expected 3, got 2", failed.getMessage());
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
    @DisplayName("an assertError keeps its own steps; the segment before it closes, and the inner assert's pass is told"
            + " before the raising verdict fails")
    void anAssertErrorKeepsItsOwnSteps() {
        TestPlan plan = fused(AN_ASSERT_ERROR);
        List<TestPlan.Verdicts> groups = groups(plan);
        assertEquals(3, groups.size(), "the segment's group, the host's step of the inner assert, the raising one");
        assertTrue(groups.get(2).agreements().get(0).raising(), "the raising verdict's agreement");
        // the message 'x' is not the inner assert's: it fails, under both plans alike
        Told fusedTold = new Told();
        AssertFailed fusedFailed = assertThrows(AssertFailed.class, () -> run(plan, fusedTold));
        // parity with the HOST judge's own run of the body (G2; the PR1 re-review's A5, 2026-11-16: until DE4 this
        // compared with the second road's plan, deleted -- the host judge's plan is the reference now): the same
        // failure text and the same verdicts told, in order
        TestPlan hostPlan = com.legend.TestExecution.planBody(typed(AN_ASSERT_ERROR), StorelessRuntime.RUNTIME,
                ExecuteOptions.NONE.withJudgeMode(PlanningOptions.JudgeMode.HOST));
        assertEquals(TestPlan.Judge.HOST, hostPlan.judge());
        Told hostTold = new Told();
        AssertFailed hostFailed = assertThrows(AssertFailed.class, () -> run(hostPlan, hostTold));
        assertEquals(hostFailed.getMessage(), fusedFailed.getMessage(), "the failure in the host's words");
        assertEquals(hostTold.verdicts, fusedTold.verdicts, "the verdicts told");
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
    @DisplayName("a segment decided at planning plans on ONE road (DE4 (a)): the decided step carries each root's host"
            + " verdict and its agreement, and both judges take the planned outcome")
    void aDecidedSegmentPlansOnOneRoad() {
        String body = "{| assertIs(Integer, Integer);}";
        TypedQuery typed = typed(body);
        ExecuteOptions options = ExecuteOptions.NONE.withJudgeMode(PlanningOptions.JudgeMode.BOTH);
        BodyDriver.ExecEnv env = BodyDriver.bodyEnv(typed, StorelessRuntime.RUNTIME,
                Databases.dialect(Databases.PLATFORM), options.planning());
        // the orchestrator's ruling under the user's delegation, 2026-11-15 (docs/records/pr1-rulings-2026-11-15.md, R1)
        assertNotNull(AgreementAsserts.planFused(typed, StorelessRuntime.RUNTIME, env, null, false),
                body + ": a decided segment plans the fused program");
        TestPlan plan = fused(body);
        assertEquals(TestPlan.Judge.AGREEMENT, plan.judge());
        List<TestPlan.Verdicts> groups = groups(plan);
        assertEquals(1, groups.size(), body + ": the one decided step");
        assertNull(groups.get(0).fused(), body + ": a decided step sends no fused statement");
        assertEquals(1, groups.get(0).agreements().size(), body);
        Told told = new Told();
        run(plan, told);
        assertEquals(List.of("assertIs pass"), told.verdicts);
    }

    @Test
    @DisplayName("DE4 (d), refused by name: a root whose host columns read the body's store, its fused row on another"
            + " session; the same root on the body's own session is carried")
    void aStoreBoundRootOnAnotherSessionIsRefusedByName() {
        String body = "{| assertEquals(1, 1);}";
        TypedQuery typed = typed(body);
        ExecuteOptions options = ExecuteOptions.NONE.withJudgeMode(PlanningOptions.JudgeMode.BOTH);
        BodyDriver.ExecEnv env = BodyDriver.bodyEnv(typed, StorelessRuntime.RUNTIME,
                Databases.dialect(Databases.PLATFORM), options.planning());
        HostAsserts host = java.util.Objects.requireNonNull(HostAsserts.plan(typed, StorelessRuntime.RUNTIME, env));
        HostAsserts.Planned hp = java.util.Objects.requireNonNull(host.planned(0));
        int carrier = hp.verdict().id();
        com.legend.executionplan.ExecutionPlan.Target body0 = env.target();
        com.legend.executionplan.ExecutionPlan.Target other = new com.legend.executionplan.ExecutionPlan.Target(
                body0.database(), List.of("-- another session"), body0.loads(), body0.raiseReadBack(),
                body0.settings(), body0.transaction());
        TestPlan.Verdicts step = new TestPlan.Verdicts(List.of(hp.verdict()), null, null,
                options.planning().judgingRules());
        List<DatabaseAsserts.HostRoot> roots = List.of(new DatabaseAsserts.HostRoot(0, hp, Map.of(), true));
        com.legend.testexecutionplan.RootLoop loop = new com.legend.testexecutionplan.RootLoop(List.of(), Map.of());
        VerdictBatch.Planned elsewhere = new VerdictBatch.Planned(List.of(new VerdictBatch.Group(step, other, false)),
                loop, Map.of(0, carrier), java.util.Set.of());
        String why = DatabaseAsserts.uncarried(elsewhere, roots, env);
        assertNotNull(why, "a store-bound root on another session");
        assertTrue(why.contains("(DE4 d)") && why.contains("another session"), why);
        // and the segment's close refuses it by that name, at planning (the PR1 re-review's B8): one root in its loop,
        // as the host's half attaches one
        com.legend.testexecutionplan.RootLoop oneRoot = new com.legend.testexecutionplan.RootLoop(List.of(
                new com.legend.testexecutionplan.RootLoop.Root(hp.verdict().name(), List.of(
                        new com.legend.testexecutionplan.RootLoop.Row(hp.verdict().name(), carrier, null, null,
                                com.legend.testexecutionplan.RootLoop.NO_APPEAL, null)), null)), Map.of());
        VerdictBatch.Planned elsewhereOneRoot = new VerdictBatch.Planned(List.of(new VerdictBatch.Group(step, other,
                false)), oneRoot, Map.of(0, carrier), java.util.Set.of());
        com.legend.error.NotImplementedException refused = assertThrows(com.legend.error.NotImplementedException.class,
                () -> DatabaseAsserts.closePlanned(elsewhereOneRoot, "b8.", 0, 0, env, new java.util.HashMap<>(),
                        new ArrayList<>(roots)));
        assertEquals(why, refused.getMessage());
        VerdictBatch.Planned here = new VerdictBatch.Planned(List.of(new VerdictBatch.Group(step, body0, false)),
                loop, Map.of(0, carrier), java.util.Set.of());
        assertNull(DatabaseAsserts.uncarried(here, roots, env), "on the body's own session it is carried");
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
            // a rejected statement is reported by the names its verdicts carry: the same under both judges (PR1's
            // finding A, 2026-11-16: BOTH had carried the host's qualified names)
            assertEquals(db.verdicts().stream().map(TestPlan.Verdict::name).toList(),
                    both.verdicts().stream().map(TestPlan.Verdict::name).toList(), body);
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
