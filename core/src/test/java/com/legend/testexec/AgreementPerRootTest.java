// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Executions;
import com.legend.database.Databases;
import com.legend.error.AssertFailed;
import com.legend.exec.StatementOrigin;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import com.legend.testexecutionplan.Agreement;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.Assertion;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.SideShape;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE AGREEMENT JUDGE PER ROOT (E-5c, step 5b, the user's ruling O5 of 2026-11-13): one fused verdict group carrying
 * TWO asserts -- a segment's loop of two roots, one agreement per root, each assert's single-value side a typed column
 * of the fused row -- run through the real test runner on the in-process database. Each root is agreed at the
 * segment's closing verdict, in root order: the host's held verdict for that root against the database half's verdict
 * for that root alone; the first failure raises, after the roots before it were decided and told.
 */
final class AgreementPerRootTest {

    private static final java.util.concurrent.atomic.AtomicInteger VERDICT_IDS =
            new java.util.concurrent.atomic.AtomicInteger(1000);

    private static Connection conn;
    private static ExecutionPlan.Target target;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
        target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
                com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null), List.of(), List.of(), null, null);
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static ExecutionPlan.Column col(String name, String type, String slot) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(1, 1), slot, null, null);
    }

    private static ExecutionPlan.Column optional(String name, String type, String slot) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(0, 1), slot, null, null);
    }

    /** One root's facts: the database's verdict row holds or not, and the host's single Boolean value. */
    private record Root(boolean databaseHolds, boolean hostValue) {
    }

    /** The fused row of verdict {@code ix}: the verdict schema's seven cells, then each root's host column, its own
     *  value on its own row and a typed NULL on the other's. */
    private static String row(int ix, Root r, int roots) {
        StringBuilder sb = new StringBuilder("SELECT " + ix + " AS ix, " + r.databaseHolds() + " AS verdict,"
                + " 'x' AS expected, 'y' AS actual, CAST(NULL AS VARCHAR) AS unjudged, FALSE AS lenient, "
                + r.databaseHolds() + " AS holds, " + (r.databaseHolds() ? "CAST(NULL AS VARCHAR)" : "'db says no'")
                + " AS message");
        for (int k = 0; k < roots; k++) {
            sb.append(", ").append(k == ix ? String.valueOf(r.hostValue()) : "CAST(NULL AS BOOLEAN)")
                    .append(" AS __h").append(k).append("_0");
        }
        return sb.toString();
    }

    /** One AGREEMENT plan: one fused group whose verdicts are {@code roots}' asserts, a loop of one root each. */
    private static TestPlan plan(Root... roots) {
        List<ExecutionPlan.Column> columns = new ArrayList<>();
        columns.add(col("ix", "Integer", "INTEGER"));
        columns.addAll(List.of(col("verdict", "Boolean", "BOOLEAN"), col("expected", "String", "VARCHAR"),
                col("actual", "String", "VARCHAR"), optional("unjudged", "String", "VARCHAR"),
                col("lenient", "Boolean", "BOOLEAN"), col("holds", "Boolean", "BOOLEAN"),
                optional("message", "String", "VARCHAR")));
        List<String> rows = new ArrayList<>();
        for (int k = 0; k < roots.length; k++) {
            columns.add(optional("__h" + k + "_0", "Boolean", "BOOLEAN"));
            rows.add(row(k, roots[k], roots.length));
        }
        ExecutionPlan.TdsResult fused = new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION, columns,
                ExecutionPlan.Sql.of(String.join(" UNION ALL ", rows), target));
        SideShape oneBoolean = new SideShape(SideShape.Kind.VALUES, "Boolean", List.of(), List.of(), false, true, 1,
                "[1]", false, false, false);
        List<TestPlan.Verdict> verdicts = new ArrayList<>();
        List<RootLoop.Root> loop = new ArrayList<>();
        List<Agreement> agreements = new ArrayList<>();
        for (int k = 0; k < roots.length; k++) {
            int id = VERDICT_IDS.incrementAndGet();
            String name = "assert@" + (k + 2);
            verdicts.add(new TestPlan.Verdict(id, name, "assert",
                    List.of(new TestPlan.Side.FusedCell(List.of(optional("__h" + k + "_0", "Boolean", "BOOLEAN")))),
                    null, null, false, new TestPlan.Host(new Assertion(name, "assert", true, null),
                            new HostJudgment.Condition("assert", true), List.of(oneBoolean))));
            loop.add(new RootLoop.Root(name, List.of(new RootLoop.Row(name, id, null, null, RootLoop.NO_APPEAL, null)),
                    null));
            agreements.add(new Agreement(k, id, RootLoop.NO_VERDICT));
        }
        TestPlan.Verdicts group = new TestPlan.Verdicts(verdicts, fused, null, TestPlan.JudgingRules.LITE,
                new RootLoop(loop, Map.of()), agreements);
        TestPlan plan = new TestPlan(List.of(), List.of(group), TestPlan.Judge.AGREEMENT, Map.of(), Map.of(), false,
                target);
        assertNull(TestPlanJson.roundTripDifference(plan), "the plan reads back as written");
        return plan;
    }

    /** The verdicts the listener was told, in order: the assert's name, then "pass" or "fail: " and its message. */
    private static final class Told implements AssertListener {
        final List<String> verdicts = new ArrayList<>();

        @Override
        public void verdict(String assertName, boolean pass, String message) {
            verdicts.add(assertName + (pass ? " pass" : " fail: " + message));
        }
    }

    private static void run(TestPlan plan, Told told) {
        TestRunner.run(new MintedPlan(plan, Executions.of(UUID.randomUUID(), Map.of())), TestSessions.over(conn),
                com.legend.executionplan.DataStores.none(), null, told, null, null, StatementOrigin.OTHER);
    }

    @Test
    @DisplayName("two asserts in one fused group, both judges hold both: each told passed, in root order")
    void bothRootsHold() {
        Told told = new Told();
        run(plan(new Root(true, true), new Root(true, true)), told);
        assertEquals(List.of("assert@2 pass", "assert@3 pass"), told.verdicts);
    }

    @Test
    @DisplayName("the second assert fails by both judges: the first passes, the second fails in the host's words")
    void theSecondRootFails() {
        Told told = new Told();
        AssertFailed failed = assertThrows(AssertFailed.class,
                () -> run(plan(new Root(true, true), new Root(false, false)), told));
        assertEquals("Assert failed", failed.getMessage(), "a failure both judges agree on: the host's words");
        assertEquals(List.of("assert@2 pass", "assert@3 fail: Assert failed"), told.verdicts);
    }

    @Test
    @DisplayName("the database half fails where the host holds, on the second: that root fails, the judges disagree")
    void theSecondRootsJudgesDisagree() {
        Told told = new Told();
        AssertFailed failed = assertThrows(AssertFailed.class,
                () -> run(plan(new Root(true, true), new Root(false, true)), told));
        assertTrue(failed.getMessage().startsWith("assert: THE JUDGES DISAGREE — the host judge holds, the database"
                + " judge fails (db says no)"), failed.getMessage());
        assertEquals(List.of("assert@2 pass", "assert@3 fail: " + failed.getMessage()), told.verdicts);
    }

    @Test
    @DisplayName("the host fails where the database holds, on the first: it fails by the agreement rule, and the"
            + " second is never decided (the first failure raises)")
    void theFirstRootsJudgesDisagree() {
        Told told = new Told();
        AssertFailed failed = assertThrows(AssertFailed.class,
                () -> run(plan(new Root(true, false), new Root(true, true)), told));
        assertTrue(failed.getMessage().startsWith("assert: THE JUDGES DISAGREE — the host judge fails (Assert failed),"
                + " the database judge holds"), failed.getMessage());
        assertEquals(List.of("assert@2 fail: " + failed.getMessage()), told.verdicts);
    }

    @Test
    @DisplayName("a group's agreements pair its loop's roots, one each, in root order: else the group is refused")
    void agreementsPairTheirRootsInOrder() {
        TestPlan two = plan(new Root(true, true), new Root(true, true));
        TestPlan.Verdicts group = (TestPlan.Verdicts) two.steps().get(0);
        List<Agreement> swapped = List.of(group.agreements().get(1), group.agreements().get(0));
        IllegalArgumentException order = assertThrows(IllegalArgumentException.class,
                () -> group.withAgreements(swapped));
        assertTrue(order.getMessage().contains("root order"), order.getMessage());
        IllegalArgumentException one = assertThrows(IllegalArgumentException.class,
                () -> group.withAgreements(List.of(group.agreements().get(0))));
        assertTrue(one.getMessage().contains("one per root"), one.getMessage());
    }
}
