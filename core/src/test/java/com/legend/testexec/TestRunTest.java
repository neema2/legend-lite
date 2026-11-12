// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import com.legend.database.Databases;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.ProgramRunner;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.TestPlanJson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A TEST PLAN RUN (the plan-as-data design's step 3, 2026-11-08; until then exec's ProgramRunnerTest, the planner-driver
 * seam program's S1b): a verdict group as a step of the test plan, run through the runner's callback, its rows to the
 * judges as the plan's judge asks -- the DATABASE judge's rows from the fused statement (a rejected one failing the
 * program by name, nothing retried: P1.2, 2026-11-07), the HOST judge's sides as typed rows (a constant side as its
 * text cells) -- and the program's answer after the steps; the test plan read back from its JSON. These plans are built
 * by hand on the platform's database.
 */
final class TestRunTest {

    /** Each verdict its own id, as a body's counter gives them (the plan-as-data design's step 0). */
    private static final java.util.concurrent.atomic.AtomicInteger VERDICT_IDS =
            new java.util.concurrent.atomic.AtomicInteger();

    private static int nextVerdictId() {
        return VERDICT_IDS.incrementAndGet();
    }

    private static Connection conn;
    private static ExecutionPlan.Target target;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
        target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null),
                List.of(), List.of(), null, null, null);
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static PlanSessions.Source sessions() {
        return PlanSessions.acquired(TestSessions.over(conn), target);
    }

    private static ExecutionPlan.Column col(String name, String type, String slot) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(1, 1), slot, null, null);
    }

    private static ExecutionPlan.Column optional(String name, String type, String slot) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(0, 1), slot, null, null);
    }

    /** The verdict schema: the verdict, the expected and actual texts, an unjudged reason, a leniency flag. */
    private static List<ExecutionPlan.Column> verdictColumns() {
        return List.of(col("verdict", "Boolean", "BOOLEAN"), col("expected", "String", "VARCHAR"),
                col("actual", "String", "VARCHAR"), optional("unjudged", "String", "VARCHAR"),
                col("lenient", "Boolean", "BOOLEAN"));
    }

    private static ExecutionPlan.TdsResult relation(String sql, List<ExecutionPlan.Column> columns) {
        return new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION, columns,
                ExecutionPlan.Sql.of(sql, target));
    }

    private static ExecutionPlan.TdsResult wrapper(boolean verdict, String expected, String actual) {
        return relation("SELECT " + verdict + " AS verdict, '" + expected + "' AS expected, '" + actual
                + "' AS actual, CAST(NULL AS VARCHAR) AS unjudged, FALSE AS lenient", verdictColumns());
    }

    private static ExecutionPlan.TdsResult fused(String sql) {
        List<ExecutionPlan.Column> columns = new ArrayList<>();
        columns.add(col("ix", "Integer", "INTEGER"));
        columns.addAll(verdictColumns());
        return relation(sql, columns);
    }

    private static final String FUSED = "SELECT 0 AS ix, TRUE AS verdict, 'x' AS expected, 'x' AS actual,"
            + " CAST(NULL AS VARCHAR) AS unjudged, FALSE AS lenient"
            + " UNION ALL SELECT 1, FALSE, 'a', 'b', CAST(NULL AS VARCHAR), FALSE";

    /** Two asserts: one holding, one not; each with a base of one rows side and one constant side. */
    private static TestPlan.Verdicts group(ExecutionPlan.TdsResult fused) {
        TestPlan.Side left = new TestPlan.Side.Rows(relation("SELECT 3 AS value",
                List.of(col("value", "Integer", "INTEGER"))));
        TestPlan.Side right = new TestPlan.Side.Constant(List.of(col("value", "Integer", null)),
                List.of(List.of("3")), false);
        TestPlan.Verdict first = new TestPlan.Verdict(nextVerdictId(), "assertEquals@2", "assertEquals",
                List.of(left, right), wrapper(true, "x", "x"));
        TestPlan.Verdict second = new TestPlan.Verdict(nextVerdictId(), "assertEquals@3", "assertEquals",
                List.of(left, right), wrapper(false, "a", "b"));
        return new TestPlan.Verdicts(List.of(first, second), fused, TestPlan.JudgingRules.LITE);
    }

    /** What the judges and the program's sink received, in order. */
    private static class Recording implements ProgramRunner.ProgramSink, TestRun.Judges {
        final List<String> events = new ArrayList<>();
        final List<List<Object>> verdictRows = new ArrayList<>();
        final List<List<PlanRunner.Rows>> sides = new ArrayList<>();
        List<ExecutionPlan.Column> answerColumns;
        final List<List<Object>> answerRows = new ArrayList<>();

        @Override
        public void verdict(TestPlan.Verdict verdict, List<Object> row) {
            events.add("verdict " + verdict.name());
            verdictRows.add(row);
        }

        @Override
        public void sides(TestPlan.Verdict verdict, List<PlanRunner.Rows> rows) {
            events.add("sides " + verdict.name());
            sides.add(rows);
        }

        @Override
        public void sidesFailed(TestPlan.Verdict verdict, RuntimeException failure) {
            throw failure;
        }

        @Override
        public void reader(SideReader reader) {
        }

        @Override
        public void fused(TestPlan.Verdicts group, com.legend.error.DataError rejected) {
            events.add(rejected == null ? "fused" : "fused rejected: " + rejected.getMessage().split("\n")[0]);
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) {
            events.add("columns");
            answerColumns = columns;
        }

        @Override
        public void batch(com.legend.result.Batch batch) {
            events.add("batch");
            for (int i = 0; i < batch.size(); i++) {
                answerRows.add(batch.row(i));
            }
        }

        @Override
        public void end() {
            events.add("end");
        }

        @Override
        public void writing(int step) {
            events.add("writing " + step);
        }
    }

    /** {@code steps} as a test plan judged by {@code judge}, read back from its JSON as written. */
    private static TestPlan plan(TestPlan.Judge judge, TestPlan.TestStep... steps) {
        TestPlan plan = new TestPlan(List.of(), List.of(steps), judge, Map.of(), Map.of(), false, target);
        assertEquals(plan, TestPlanJson.read(TestPlanJson.write(plan)), "the plan reads back as written");
        return plan;
    }

    private static void run(TestPlan plan, PlanSessions.Source sessions, Recording sink) throws IOException {
        TestRun.run(plan, Map.of(), sessions, com.legend.executionplan.DataStores.none(), step -> null, sink, sink, com.legend.base.Executions.one(java.util.UUID.randomUUID()));
    }

    @Test
    @DisplayName("a program's own load (AT_STEP) is a write of the body's")
    void aProgramsOwnLoadIsAWrite() throws IOException {
        // the runner tells the sink of a write at an AT_STEP load (a live source's session table, SOURCE, no write,
        // went with the JSON pass, 2026-11-11: a data: payload is bound or inline)
        ExecutionPlan.Load own = com.legend.planner.LoadPlanner.load(ExecutionPlan.Load.Cadence.AT_STEP, List.of(
                new ExecutionPlan.LoadStep.Statement("DROP TABLE IF EXISTS temp.legend_own_load"),
                new ExecutionPlan.LoadStep.Statement("CREATE TEMPORARY TABLE legend_own_load (x INTEGER)")), target);
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.HOST, new TestPlan.Run(own),
                new TestPlan.Run(relation("SELECT 1 AS value", List.of(col("value", "Integer", "INTEGER"))))),
                sessions(), sink);
        assertTrue(sink.events.contains("writing 0"), sink.events.toString());
    }

    @Test
    @DisplayName("a verdict's message is never read on its own: the judge reads it on demand, through the run's reader")
    void aVerdictsMessageIsReadOnlyOnDemand() throws IOException {
        TestPlan.Side base = new TestPlan.Side.Constant(List.of(col("value", "Integer", null)),
                List.of(List.of("3")), false);
        TestPlan.Side message = new TestPlan.Side.Rows(relation("SELECT 'read on demand' AS value",
                List.of(col("value", "String", "VARCHAR"))));
        TestPlan.Side raising = new TestPlan.Side.Rows(relation("SELECT * FROM no_such_table_message",
                List.of(col("value", "String", "VARCHAR"))));
        TestPlan.Verdicts group = new TestPlan.Verdicts(List.of(
                new TestPlan.Verdict(nextVerdictId(), "assert@1", "assert", List.of(base), null, raising),
                new TestPlan.Verdict(nextVerdictId(), "assert@2", "assert", List.of(base), null, message)),
                null, TestPlan.JudgingRules.LITE);
        List<Object> read = new ArrayList<>();
        SideReader[] reader = new SideReader[1];
        Recording sink = new Recording() {
            @Override
            public void reader(SideReader r) {
                reader[0] = r;
            }

            @Override
            public void sides(TestPlan.Verdict verdict, List<PlanRunner.Rows> rows) {
                super.sides(verdict, rows);
                if (verdict.name().equals("assert@2")) {
                    try {
                        read.add(reader[0].read(verdict.message()).rows().get(0).get(0));
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
            }
        };
        // the first verdict's message would raise if sent: the run never reads it on its own
        run(plan(TestPlan.Judge.HOST, group), sessions(), sink);
        assertEquals(List.of("sides assert@1", "sides assert@2"), sink.events);
        assertEquals(1, sink.sides.get(0).size(), "the base alone, the message not read");
        assertEquals(List.of("read on demand"), read);
        assertNull(reader[0], "the reader closed with the program");
    }

    @Test
    @DisplayName("database judging: the fused statement's rows, one per verdict, in the group's order; the judges told"
            + " it ran")
    void databaseJudgingReadsTheFusedStatement() throws IOException {
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.DATABASE, group(fused(FUSED))), sessions(), sink);
        assertEquals(List.of("fused", "verdict assertEquals@2", "verdict assertEquals@3"), sink.events);
        assertEquals(java.util.Arrays.asList(true, "x", "x", null, false), cells(sink.verdictRows.get(0)));
        assertEquals(java.util.Arrays.asList(false, "a", "b", null, false), cells(sink.verdictRows.get(1)));
    }

    @Test
    @DisplayName("the database rejects the fused statement: the judges told, the program fails by the verdicts' names,"
            + " no wrapper run in its place (P1.2: exec runs only the plan's statements)")
    void aRejectedFusedStatementFailsTheProgram() {
        Recording sink = new Recording();
        com.legend.error.DataError e = assertThrows(com.legend.error.DataError.class,
                () -> run(plan(TestPlan.Judge.DATABASE, group(fused("SELECT * FROM no_such_table_s1b"))), sessions(),
                        sink));
        assertEquals(1, sink.events.size(), sink.events.toString());
        assertTrue(sink.events.get(0).startsWith("fused rejected: "), sink.events.get(0));
        // the database's own error first, the verdicts named after it (round 25, 2026-11-09)
        assertTrue(e.getMessage().startsWith(e.getCause().getMessage()), e.getMessage());
        assertTrue(e.getMessage().endsWith(" (the database rejected the verdict statement of [assertEquals@2,"
                + " assertEquals@3])"), e.getMessage());
        assertTrue(sink.verdictRows.isEmpty(), "no wrapper ran in the fused statement's place");
    }

    @Test
    @DisplayName("no fused statement: each verdict's wrapper runs; a verdict without one is the host judge's: its sides"
            + " read, no row")
    void noFusedStatementRunsEachWrapper() throws IOException {
        TestPlan.Verdicts g = group(null);
        TestPlan.Verdict unjudged = new TestPlan.Verdict(nextVerdictId(), "assertIs@4", "assertIs",
                g.verdicts().get(0).base(), null);
        TestPlan.Verdicts withUnjudged = new TestPlan.Verdicts(
                List.of(g.verdicts().get(0), unjudged, g.verdicts().get(1)), null, TestPlan.JudgingRules.LITE);
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.DATABASE, withUnjudged), sessions(), sink);
        // 2026-11-03 (assertError an ordinary verdict under the database judge, ruling N3): a verdict with sides and no
        // wrapper is the HOST judge's in a database-judged program -- its sides read (and only its: no fused statement
        // and no other verdict's sides are sent for it), then every verdict in the group's order, its own with no row
        assertEquals(List.of("sides assertIs@4", "verdict assertEquals@2", "verdict assertIs@4",
                "verdict assertEquals@3"), sink.events);
        assertEquals(java.util.Arrays.asList(true, "x", "x", null, false), cells(sink.verdictRows.get(0)));
        assertNull(sink.verdictRows.get(1), "a verdict with no wrapper: the database judges nothing, the host its sides");
        assertEquals(java.util.Arrays.asList(false, "a", "b", null, false), cells(sink.verdictRows.get(2)));
    }

    @Test
    @DisplayName("host judging: each verdict's sides as typed rows, a constant side its text cells; nothing fused is sent")
    void hostJudgingHandsTheSides() throws IOException {
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.HOST, group(fused(FUSED))), sessions(), sink);
        assertEquals(List.of("sides assertEquals@2", "sides assertEquals@3"), sink.events);
        List<PlanRunner.Rows> first = sink.sides.get(0);
        assertEquals(2, first.size());
        assertEquals(List.of(3L), first.get(0).rows().get(0), "the rows side, read from the database");
        assertEquals(List.of("3"), first.get(1).rows().get(0), "the constant side, its text cell as written");
        assertEquals("Integer", first.get(1).columns().get(0).type());
    }

    @Test
    @DisplayName("both: the sides first, then the database rows, per group")
    void bothJudgesHandsSidesThenRows() throws IOException {
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.AGREEMENT, group(fused(FUSED))), sessions(), sink);
        assertEquals(List.of("sides assertEquals@2", "sides assertEquals@3", "fused", "verdict assertEquals@2",
                "verdict assertEquals@3"), sink.events);
    }

    @Test
    @DisplayName("a program with an answer: the verdicts, then the result step's rows after the steps")
    void aProgramsAnswerFollowsItsVerdicts() throws IOException {
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.DATABASE, new TestPlan.Run(new ExecutionPlan.Check(ExecutionPlan.Sql.of("SELECT 1",
                target))), group(fused(FUSED)), new TestPlan.Run(relation("SELECT 7 AS value",
                List.of(col("value", "Integer", "INTEGER"))))), sessions(), sink);
        assertEquals(List.of("fused", "verdict assertEquals@2", "verdict assertEquals@3", "columns", "batch", "end"),
                sink.events);
        assertEquals(List.of(List.of(7L)), sink.answerRows);
        assertEquals("value", sink.answerColumns.get(0).name());
    }

    @Test
    @DisplayName("a program may answer no rows: a body ending in an assert")
    void aProgramMayAnswerNoRows() throws IOException {
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.DATABASE, new TestPlan.Run(new ExecutionPlan.Check(ExecutionPlan.Sql.of("SELECT 1",
                target))), group(fused(FUSED))), sessions(), sink);
        assertEquals(List.of("fused", "verdict assertEquals@2", "verdict assertEquals@3"), sink.events);
        assertNull(sink.answerColumns);
    }

    @Test
    @DisplayName("the test plan reads back from its JSON: every side kind, a wrapper, a fused statement, no wrapper")
    void aVerdictGroupReadsBackFromItsJson() {
        ExecutionPlan.Sql keys = ExecutionPlan.Sql.of("SELECT 'k' AS k, 'k__|__v' AS n", target);
        String bodyText = "SELECT 1 AS a, ? AS _pk0_v";
        ExecutionPlan.Sql body = new ExecutionPlan.Sql(bodyText,
                List.of(new ExecutionPlan.Slot("", new ExecutionPlan.Binding.Key(0))), target, null,
                List.of(new ExecutionPlan.Region(13, bodyText.length(), "", ", ", "", 0, 1, List.of())));
        ExecutionPlan.Keyed keyed = new ExecutionPlan.Keyed(keys, List.of("k", "n"),
                List.of(new ExecutionPlan.Keyed.Fixed(col("a", "Integer", "INTEGER"), "a"),
                        new ExecutionPlan.Keyed.PerKey(col("v", "Integer", "INTEGER"), 1, "v")),
                1, body, ExecutionPlan.Keyed.Snapshot.TRANSACTION, null);
        TestPlan.Side keyedSide = new TestPlan.Side.KeyedRows(keyed);
        TestPlan.Verdicts g = group(fused(FUSED));
        TestPlan.Verdict third = new TestPlan.Verdict(nextVerdictId(), "assertTdsEquivalent@5", "assertTdsEquivalent",
                List.of(keyedSide, new TestPlan.Side.Constant(List.of(col("a", "Integer", null),
                        optional("b", "String", null)), List.of(List.of("1", "x"), java.util.Arrays.asList("2", null)),
                        true)),
                null);
        TestPlan plan = new TestPlan(List.of(), List.of(
                new TestPlan.Run(new ExecutionPlan.Check(ExecutionPlan.Sql.of("SELECT 1", target))),
                new TestPlan.Verdicts(List.of(g.verdicts().get(0), g.verdicts().get(1), third), g.fused(),
                        TestPlan.JudgingRules.LITE),
                new TestPlan.Run(relation("SELECT 7 AS value", List.of(col("value", "Integer", "INTEGER"))))),
                TestPlan.Judge.DATABASE, Map.of(), Map.of(), false, target);
        String json = TestPlanJson.write(plan);
        assertEquals(plan, TestPlanJson.read(json), "the plan read back differs");
        assertTrue(json.contains("\"_type\":\"verdicts\"") && json.contains("\"_type\":\"keyedRows\"")
                && json.contains("\"_type\":\"constant\"") && json.contains("\"folded\":true"), json);
    }

    // ---- a branch (the asserts-as-plan-nodes program, step 4, 2026-11-04): an if whose sides hold asserts ----------

    /** One assert whose one rows side reads {@code sql}. */
    private static TestPlan.Verdicts oneVerdict(String name, String sql) {
        TestPlan.Side side = new TestPlan.Side.Rows(relation(sql, List.of(col("value", "Integer", "INTEGER"))));
        return new TestPlan.Verdicts(List.of(new TestPlan.Verdict(nextVerdictId(), name, "assert", List.of(side), null)),
                null, TestPlan.JudgingRules.LITE);
    }

    @Test
    @DisplayName("a branch: the condition read, the side it names run, the other never sent")
    void aBranchRunsTheSideItsConditionNames() throws IOException {
        TestPlan.Side condition = new TestPlan.Side.Rows(relation("SELECT 1 > 2 AS value",
                List.of(col("value", "Boolean", "BOOLEAN"))));
        // the side not taken reads a table that does not exist: sent, it would fail the program
        TestPlan.Branch branch = new TestPlan.Branch(condition,
                List.of(oneVerdict("then@1", "SELECT v AS value FROM no_such_table_branch")),
                List.of(oneVerdict("else@1", "SELECT 4 AS value")));
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.HOST, branch), sessions(), sink);
        assertEquals(List.of("sides else@1"), sink.events);
        assertEquals(List.of(4L), sink.sides.get(0).get(0).rows().get(0));
    }

    @Test
    @DisplayName("a branch that sends nothing: its constant condition read from the program's text, no session opened")
    void aConstantBranchRunsWithNoSession() throws IOException {
        TestPlan.Side yes = new TestPlan.Side.Constant(List.of(col("value", "Boolean", null)),
                List.of(List.of("true")), false);
        TestPlan.Side three = new TestPlan.Side.Constant(List.of(col("value", "Integer", null)),
                List.of(List.of("3")), false);
        TestPlan.Verdicts then = new TestPlan.Verdicts(List.of(new TestPlan.Verdict(nextVerdictId(), "then@1",
                "assertEquals", List.of(three, three), null)), null, TestPlan.JudgingRules.LITE);
        TestPlan.Verdicts otherwise = new TestPlan.Verdicts(List.of(new TestPlan.Verdict(nextVerdictId(), "else@1",
                "assertEquals", List.of(three, three), null)), null, TestPlan.JudgingRules.LITE);
        TestPlan.Branch inner = new TestPlan.Branch(yes, List.of(), List.of(otherwise));
        TestPlan.Branch branch = new TestPlan.Branch(yes, List.of(then, inner), List.of(otherwise));
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.AGREEMENT, branch), PlanSessions.shared(), sink);
        assertEquals(List.of("sides then@1", "verdict then@1"), sink.events);
    }

    @Test
    @DisplayName("a program of constants alone under the database judge hands a host-judged verdict its sides")
    void aConstantHostVerdictGetsItsSidesUnderTheDatabaseJudge() throws IOException {
        // an assertError's step is the host's in both modes (ruling N3): with no wrapper and constant sides, the runner's
        // constants-alone path handed it no sides until 2026-11-04, so it was never judged under the database judge
        TestPlan.Side three = new TestPlan.Side.Constant(List.of(col("value", "Integer", null)),
                List.of(List.of("3")), false);
        TestPlan.Verdicts step = new TestPlan.Verdicts(List.of(new TestPlan.Verdict(nextVerdictId(), "assertError@1",
                "assertError", List.of(three), null)), null, TestPlan.JudgingRules.LITE);
        Recording sink = new Recording();
        run(plan(TestPlan.Judge.DATABASE, step), PlanSessions.shared(), sink);
        assertEquals(List.of("sides assertError@1", "verdict assertError@1"), sink.events);
    }

    @Test
    @DisplayName("a branch reads back from its JSON, and a plan of branches answers no rows")
    void aBranchReadsBackFromItsJson() {
        TestPlan.Side condition = new TestPlan.Side.Rows(relation("SELECT TRUE AS value",
                List.of(col("value", "Boolean", "BOOLEAN"))));
        TestPlan.Branch inner = new TestPlan.Branch(new TestPlan.Side.Constant(
                List.of(col("value", "Boolean", null)), List.of(List.of("false")), false),
                List.of(group(fused(FUSED))), List.of());
        TestPlan plan = new TestPlan(List.of(), List.of(
                new TestPlan.Branch(condition, List.of(oneVerdict("then@1", "SELECT 1 AS value"), inner),
                        List.of(oneVerdict("else@1", "SELECT 2 AS value")))), TestPlan.Judge.HOST, Map.of(), Map.of(),
                false, target);
        String json = TestPlanJson.write(plan);
        assertEquals(plan, TestPlanJson.read(json), "the plan read back differs");
        assertTrue(json.contains("\"_type\":\"branch\""), json);
        // the rules each verdict group is judged by are the plan's own (the explicit rule, 2026-11-04)
        assertTrue(json.contains("\"rules\":\"lite\""), json);
        assertTrue(!plan.answers(), "a plan that only branches answers no rows");
        assertThrows(IllegalStateException.class, plan::answerShape, "a plan that only branches answers no rows");
        assertThrows(IllegalArgumentException.class, () -> new TestPlan.Branch(condition,
                List.of(new TestPlan.Run(relation("SELECT 1 AS value", List.of(col("value", "Integer", "INTEGER"))))),
                List.of()), "a branch's side holds verdict groups and branches");
    }

    private static List<Object> cells(List<Object> row) {
        return new ArrayList<>(row);
    }

    // ---- F11: a verdict group's savepoint ----

    private static void exec(String sql) throws SQLException {
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static long count(String table) throws SQLException {
        try (java.sql.Statement st = conn.createStatement();
                java.sql.ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    @DisplayName("F11: a fused statement inside a transaction is sent bare -- no savepoint around it (its rejection"
            + " fails the program, whose transaction rolls back whole: P1.2)")
    void aFusedStatementInATransactionIsSentBare() throws Exception {
        // the savepoint's three texts are stand-ins that log their step (DuckDB has no savepoint)
        exec("CREATE OR REPLACE TABLE m3b_sp (step VARCHAR)");
        exec("CREATE OR REPLACE TABLE m3b_w (a INTEGER)");
        TestPlan.Verdicts.Savepoint sp = new TestPlan.Verdicts.Savepoint(
                "INSERT INTO m3b_sp VALUES ('set')", "INSERT INTO m3b_sp VALUES ('rollbackTo')",
                "INSERT INTO m3b_sp VALUES ('release')");
        TestPlan.Run write = new TestPlan.Run(new ExecutionPlan.Update(ExecutionPlan.Sql.of(
                "INSERT INTO m3b_w VALUES (1)", target), null));
        TestPlan.Verdicts held = group(fused(FUSED));
        TestPlan.Verdicts guarded = new TestPlan.Verdicts(held.verdicts(), held.fused(), sp, held.rules());
        Recording ran = new Recording();
        run(plan(TestPlan.Judge.DATABASE, write, guarded), sessions(), ran);
        assertEquals(2, ran.verdictRows.size());
        assertEquals(0, count("m3b_sp"), "no savepoint statement around the fused statement");
        TestPlan.Verdicts rejected = group(fused("SELECT * FROM no_such_table_m3b"));
        assertThrows(com.legend.error.DataError.class, () -> run(plan(TestPlan.Judge.DATABASE, write,
                new TestPlan.Verdicts(rejected.verdicts(), rejected.fused(), sp, rejected.rules())), sessions(),
                new Recording()));
        assertEquals(0, count("m3b_sp"), "the rejection sent no savepoint statement and ran no wrapper");
    }
}
