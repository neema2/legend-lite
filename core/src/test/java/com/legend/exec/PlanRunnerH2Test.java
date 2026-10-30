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
    void aSessionOfAnotherDatabaseOrServerVersionIsRefused_neverRun() throws Exception {
        DatabaseType other = TestBackends.inProcess().stream().map(TestBackend::type).filter(t -> t != H2).findFirst()
                .orElseThrow();
        ExecutionPlan another = counting(inMemory("s::Other", other, new ConnectionSpecification.InMemory()), "T",
                new ExecutionPlan.Servers.Every());
        ExecutionPlan h2ForNine = counting(inMemory("s::H2", H2, new ConnectionSpecification.LocalH2(null)),
                "T", new ExecutionPlan.Servers.Versions(List.of("9.")));
        try (Connection h2 = TestBackends.of(H2).open()) {
            var otherDatabase = assertThrows(RuntimeException.class, () -> run(another, PlanSessions.given(h2)));
            assertTrue(otherDatabase.getMessage().contains("dialect/connection mismatch"), otherDatabase.getMessage());
            var otherVersion = assertThrows(com.legend.error.NotImplementedException.class,
                    () -> run(h2ForNine, PlanSessions.given(h2)));
            assertTrue(otherVersion.getMessage().contains("written for H2 [9.], and the session is H2 2.1"),
                    otherVersion.getMessage());
        }
    }

    /** A LocalH2 target (legend-engine's own H2 specification) is shared and set up once, as every in-process
     *  database's in-memory target is ({@link PlanRunnerTest}, which declares {@code InMemory}). */
    @Test
    void aLocalH2TargetIsSharedAndSetUpOnce() throws Exception {
        ExecutionPlan plan = counting(inMemory("s::H2", H2, new ConnectionSpecification.LocalH2(null)), "LOCAL_T",
                H2_SERVERS);
        long before = setupStatements();
        assertEquals("1", run(plan, PlanSessions.shared()));
        assertEquals("1", run(plan, PlanSessions.shared()));
        assertEquals(2, setupStatements() - before, "set up once");
    }

    /** The H2 versions its plans are written for. */
    private static final ExecutionPlan.Servers H2_SERVERS = new ExecutionPlan.Servers.Versions(List.of("2.1", "2.2"));

    /** H2's spelling of each kind of value, as its dialect writes a type hole's (H2.holeType). */
    private static final Map<ExecutionPlan.ValueKind, ExecutionPlan.TypeSpelling> H2_TYPES = Map.of(
            ExecutionPlan.ValueKind.INTEGER, new ExecutionPlan.TypeSpelling("BIGINT", ExecutionPlan.Digits.NONE),
            ExecutionPlan.ValueKind.DECIMAL, new ExecutionPlan.TypeSpelling("NUMERIC",
                    ExecutionPlan.Digits.PRECISION_AND_SCALE),
            ExecutionPlan.ValueKind.FLOATING, new ExecutionPlan.TypeSpelling("DECFLOAT", ExecutionPlan.Digits.PRECISION),
            ExecutionPlan.ValueKind.DATE, new ExecutionPlan.TypeSpelling("DATE", ExecutionPlan.Digits.NONE),
            ExecutionPlan.ValueKind.DATE_TIME, new ExecutionPlan.TypeSpelling("TIMESTAMP(9)",
                    ExecutionPlan.Digits.NONE),
            ExecutionPlan.ValueKind.DATE_TIME_NANOS, new ExecutionPlan.TypeSpelling("TIMESTAMP(9)",
                    ExecutionPlan.Digits.NONE));

    /** A plan reading one value typed by its value, through a type hole, as text: {@code type} is the parameter's. */
    private static ExecutionPlan holed(String type) {
        String before = "SELECT CAST(CAST(? AS ";
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Declared(inMemory("s::H2",
                H2, new ConnectionSpecification.LocalH2(null))), H2_SERVERS, List.of(), List.of());
        return new ExecutionPlan(List.of(declared("v", type, 0, 1)), PlanRunnerTest.cell(
                new ExecutionPlan.Sql(before + ") AS VARCHAR)", List.of(new ExecutionPlan.Slot("v",
                        new ExecutionPlan.Binding.One("DECIMAL", new ExecutionPlan.TypeHole(before.length(), H2_TYPES,
                                type.equals("Date") ? ExecutionPlan.ValueKind.DATE
                                        : ExecutionPlan.ValueKind.DECIMAL)))), target, null)));
    }

    /** The runner fills a type hole with the type of the value's own literal: each value answers on H2 exactly as its
     *  literal does, text included (the whole sweep, every position: probes/value-typed-cast-results.txt). */
    @Test
    void aTypeHoleIsFilledWithTheTypeOfTheValuesLiteral_andAnswersAsTheLiteralDoes() throws Exception {
        Map<Object, String> literals = new java.util.LinkedHashMap<>();
        literals.put(1.1d, "1.1");
        literals.put(new BigDecimal("2.50"), "2.50");
        literals.put(1e-6d, "0.0000010");
        literals.put(1.5e15d, "1.5E15");
        literals.put(2.5e-7d, "2.5E-7");
        literals.put(7L, "CAST(7 AS BIGINT)");
        try (Connection h2 = TestBackends.of(H2).open()) {
            for (var e : literals.entrySet()) {
                assertEquals(literalText(h2, e.getValue()), PlanRunnerTest.run(holed("Number"), Map.of("v", e.getKey()),
                        PlanSessions.given(h2)), String.valueOf(e.getKey()));
            }
            Map<Object, String> dates = Map.of(LocalDate.of(2024, 1, 2), "DATE '2024-01-02'",
                    LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_456_789), "TIMESTAMP '2024-01-02 10:30:00.123456789'");
            for (var e : dates.entrySet()) {
                assertEquals(literalText(h2, e.getValue()), PlanRunnerTest.run(holed("Date"), Map.of("v", e.getKey()),
                        PlanSessions.given(h2)), String.valueOf(e.getKey()));
            }
            // an absent value: a null of its absent kind's type, by name alone
            assertEquals(null, PlanRunnerTest.run(holed("Number"), Map.of(), PlanSessions.given(h2)));
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
            ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Declared(inMemory("s::H2",
                    H2, new ConnectionSpecification.LocalH2(null))), H2_SERVERS, List.of(), List.of());
            ExecutionPlan plan = new ExecutionPlan(List.of(declared("n", "Integer", 1, 1)), new ExecutionPlan.TdsResult(
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
            PlanRunner.Rows rows = PlanRunner.rows(plan, Map.of("n", 7L), PlanSessions.given(h2));
            assertEquals(List.of(List.of(7L, "a", com.legend.values.PureDateLiteral.parse("2024-01-02"))), rows.rows());
            assertEquals(List.of("N", "S", "D"), rows.columns().stream().map(ExecutionPlan.Column::name).toList());
        }
    }

    /** So is an in-memory database the user named (an EmbeddedH2): the user's identity, shared by design. */
    @Test
    void aDatabaseTheUserNamedIsNeverSetUp() {
        ExecutionPlan p = counting(inMemory("s::Named", H2,
                new ConnectionSpecification.EmbeddedH2("users_own", "/never", false)), "NAMED_T", H2_SERVERS);
        var refused = assertThrows(IllegalStateException.class, () -> run(p, PlanSessions.shared()));
        assertEquals("connection 's::Named' is an in-memory database the user named ('users_own'), and the plan has"
                + " setup for it: never run on it", refused.getMessage());
    }

    /** Runs of one target that start together set it up once: its two setup statements sent once, every run answered
     *  from that database (that a run waits for a setup in progress, and that another target's is not held up, is
     *  HandleStoreTest's, deterministically). */
    @Test
    void runsOfOneTargetThatStartTogetherSetItUpOnce() throws Exception {
        ExecutionPlan p = plan("SELECT CAST(COUNT(*) AS VARCHAR) FROM TOGETHER_T",
                target(new ExecutionPlan.Database.Declared(inMemory("s::H2", H2,
                                new ConnectionSpecification.LocalH2(null))), H2_SERVERS,
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
