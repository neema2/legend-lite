// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static com.legend.exec.PlanRunnerTest.counting;
import static com.legend.exec.PlanRunnerTest.declared;
import static com.legend.exec.PlanRunnerTest.inMemory;
import static com.legend.exec.PlanRunnerTest.plan;
import static com.legend.exec.PlanRunnerTest.run;
import static com.legend.exec.PlanRunnerTest.setupStatements;
import static com.legend.exec.PlanRunnerTest.statement;
import static com.legend.exec.PlanRunnerTest.target;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The runner on H2 ({@link PlanRunnerTest}'s cases that are H2's own; moved here from it by the dialect program's
 * Phase 6, 2026-10-16): the type hole it fills (H2 types a placeholder when it prepares it), the server versions a
 * plan is written for, an in-memory database the user named, and a setup many runs wait on.
 */
class PlanRunnerH2Test {

    private static final DatabaseType H2 = DatabaseType.H2;

    @Test
    void aSessionOfAnotherDatabaseIsRefused_neverRun() throws Exception {
        DatabaseType other = TestBackends.inProcess().stream().map(TestBackend::type).filter(t -> t != H2).findFirst()
                .orElseThrow();
        ExecutionPlan another = counting(inMemory("s::Other", other, new ConnectionSpecification.InMemory()), "T");
        try (Connection h2 = TestBackends.of(H2).open()) {
            // its loads established on the session it is acquired on (a caller's session as it is refuses a target that
            // loads: the re-audit's A-3, 2026-11-09)
            var otherDatabase = assertThrows(RuntimeException.class, () -> run(another, PlanSessions.acquired(
                    com.legend.testbackend.TestSessions.over(h2), another.answerSql().target())));
            assertTrue(otherDatabase.getMessage().contains("dialect/connection mismatch"), otherDatabase.getMessage());
        }
    }

    /** A LocalH2 target (legend-engine's own H2 specification) is shared and set up once, as every in-process
     *  database's in-memory target is ({@link PlanRunnerTest}, which declares {@code InMemory}). */
    @Test
    void aLocalH2TargetIsSharedAndSetUpOnce() throws Exception {
        ExecutionPlan plan = counting(inMemory("s::H2", H2, new ConnectionSpecification.LocalH2(null)), "LOCAL_T");
        long before = setupStatements();
        assertEquals("1", run(plan, PlanSessions.shared()));
        assertEquals("1", run(plan, PlanSessions.shared()));
        assertEquals(2, setupStatements() - before, "set up once");
    }

    /** A plan reading one value of a declared kind as text, its placeholder cast to the declared kind's fixed type
     *  ({@code cast}: H2's, D1, P4) -- the statement the plan's own, never written at run. */
    private static ExecutionPlan declaredCast(String type, String cast, ExecutionPlan.@com.legend.base.Nullable AsText asText) {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Declared(com.legend.planner.ConnectionPlans.of(inMemory("s::H2",
                H2, new ConnectionSpecification.LocalH2(null))), null), List.of(), List.of(), null,
                com.legend.testbackend.TestBackends.settings(H2), null);
        return ExecutionPlan.reading(List.of(declared("v", type, 0, 1)), PlanRunnerTest.cell(
                new ExecutionPlan.Sql("SELECT CAST(CAST(? AS " + cast + ") AS VARCHAR)", List.of(new ExecutionPlan.Slot("v",
                        new ExecutionPlan.Binding.One("DECIMAL", asText))), target, null)));
    }

    /** A placeholder of a declared kind answers as the database's own cast of the value to the declared kind's fixed
     *  type (D1, P4, 2026-11-09: until then the runner wrote the type of the value's literal into a hole), the same
     *  statement for every value; a date or date-time is bound as its text, a StrictDate value a midnight timestamp. */
    @Test
    void aDeclaredKindsPlaceholderAnswersAsTheFixedTypesCast() throws Exception {
        // each value, its literal as upstream writes it bare (H2 types it by its digits), and lite's answer where it is
        // not upstream's: the DECFLOAT placeholder's, PENDING THE USER'S RULING (S175; the correctness re-audit's 3,
        // 2026-11-09: until then this compared the plan with H2's own DECFLOAT cast, which proved nothing)
        record Number(Object value, String literal, @com.legend.base.Nullable String pending) {
        }
        List<Number> numbers = List.of(new Number(1.1d, "1.1", null), new Number(new BigDecimal("2.50"), "2.50", "2.5"),
                new Number(1e-6d, "0.000001", null), new Number(7L, "7", null), new Number(100L, "100", "1E+2"),
                new Number(new BigDecimal("1000"), "1000", "1E+3"));
        try (Connection h2 = TestBackends.of(H2).open()) {
            for (Number n : numbers) {
                String upstream = literalText(h2, n.literal());
                String lite = PlanRunnerTest.run(declaredCast("Number", "DECFLOAT", null), Map.of("v", n.value()),
                        PlanSessions.of(com.legend.testbackend.TestSessions.over(h2)));
                assertEquals(n.pending() == null ? upstream : n.pending(), lite, n.literal() + " (upstream: " + upstream
                        + ")");
            }
            Map<Object, String> dates = Map.of(LocalDate.of(2024, 1, 2), "2024-01-02 00:00:00",
                    LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_456_789), "2024-01-02 10:30:00.123456789");
            for (var e : dates.entrySet()) {
                assertEquals(e.getValue(), PlanRunnerTest.run(declaredCast("Date", "TIMESTAMP(9)",
                        new ExecutionPlan.AsText()), Map.of("v", e.getKey()),
                        PlanSessions.of(com.legend.testbackend.TestSessions.over(h2))), String.valueOf(e.getKey()));
            }
            // an absent value: a null of the binding's type
            assertEquals(null, PlanRunnerTest.run(declaredCast("Number", "DECFLOAT", null), Map.of(),
                    PlanSessions.of(com.legend.testbackend.TestSessions.over(h2))));
        }
    }

    private static String literalText(Connection c, String literal) throws Exception {
        try (var st = c.createStatement(); var rs = st.executeQuery("SELECT CAST(" + literal + " AS VARCHAR)")) {
            rs.next();
            return rs.getString(1);
        }
    }

    /** A plan whose answer is typed rows: each cell read from the driver by the plan's column (the typed seam,
     *  docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3; the plan path program's step 7, 2026-10-18), the slots bound as
     *  a text plan's are; a rows plan is not run as a text. */
    @Test
    void typedRowsAreReadFromTheDriverByThePlansColumns() throws Exception {
        try (Connection h2 = TestBackends.of(H2).open()) {
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Declared(com.legend.planner.ConnectionPlans.of(inMemory("s::H2",
                    H2, new ConnectionSpecification.LocalH2(null))), null), List.of(), List.of(), null,
                com.legend.testbackend.TestBackends.settings(H2), null);
            ExecutionPlan plan = ExecutionPlan.reading(List.of(declared("n", "Integer", 1, 1)), new ExecutionPlan.TdsResult(
                    ExecutionPlan.TdsResult.Shape.RELATION,
                    List.of(new ExecutionPlan.Column("N", "Integer", new ExecutionPlan.Multiplicity(1, 1), "BIGINT", null,
                                    null),
                            new ExecutionPlan.Column("S", "String", new ExecutionPlan.Multiplicity(0, 1), "VARCHAR", null,
                                    null),
                            new ExecutionPlan.Column("D", "StrictDate", new ExecutionPlan.Multiplicity(1, 1), "DATE", null,
                                    ExecutionPlan.Precision.DAY)),
                    new ExecutionPlan.Sql("SELECT CAST(? AS BIGINT) AS N, 'a' AS S, DATE '2024-01-02' AS D",
                            List.of(new ExecutionPlan.Slot("n", new ExecutionPlan.Binding.One("BIGINT", null))), target,
                            null)));
            PlanRunner.Rows rows = PlanRunner.rows(plan, Map.of("n", 7L), PlanSessions.of(com.legend.testbackend.TestSessions.over(h2)), java.util.UUID.randomUUID());
            assertEquals(List.of(List.of(7L, "a", com.legend.values.PureDateLiteral.parse("2024-01-02"))), rows.rows());
            assertEquals(List.of("N", "S", "D"), rows.columns().stream().map(ExecutionPlan.Column::name).toList());
        }
    }

    /** So is an in-memory database the user named (an EmbeddedH2): the user's identity, shared by design. */
    @Test
    void aDatabaseTheUserNamedIsNeverSetUp() {
        ExecutionPlan p = counting(inMemory("s::Named", H2,
                new ConnectionSpecification.EmbeddedH2("users_own", "/never", false)), "NAMED_T");
        var refused = assertThrows(IllegalStateException.class, () -> run(p, PlanSessions.shared()));
        assertEquals("connection 's::Named' is an in-memory database the user named ('users_own'), and the plan loads"
                + " data into it: never run on it", refused.getMessage());
    }

    /** Runs of one target that start together set it up once: its two setup statements sent once, every run answered
     *  from that database (that a run waits for a setup in progress, and that another target's is not held up, is
     *  HandleStoreTest's, deterministically). */
    @Test
    void runsOfOneTargetThatStartTogetherSetItUpOnce() throws Exception {
        ExecutionPlan p = plan("SELECT CAST(COUNT(*) AS VARCHAR) FROM TOGETHER_T",
                target(new ExecutionPlan.Database.Declared(com.legend.planner.ConnectionPlans.of(inMemory("s::H2", H2,
                                new ConnectionSpecification.LocalH2(null))), null),
                        statement("CREATE TABLE TOGETHER_T (ID INTEGER)"),
                        statement("INSERT INTO TOGETHER_T SELECT X FROM SYSTEM_RANGE(1, 100000)")));
        int runs = 8;
        var start = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(runs);
        long before = setupStatements();
        try {
            List<java.util.concurrent.Future<String>> answers = new java.util.ArrayList<>();
            for (int i = 0; i < runs; i++) {
                answers.add(pool.submit(() -> {
                    start.await();
                    return run(p, PlanSessions.shared());
                }));
            }
            start.countDown();
            for (var answer : answers) {
                assertEquals("100000", answer.get(60, java.util.concurrent.TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(2, setupStatements() - before, "set up once for " + runs + " runs");
    }
}
