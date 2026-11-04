// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
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
 * A PROGRAM run by the runner (the planner-driver seam program's S1b, 2026-10-30,
 * docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md §3.2 to §3.4): a verdict group as a step, its rows to the program's
 * sink as the judging mode asks -- the DATABASE judge's rows from the fused statement, each verdict's own wrapper
 * where the database rejects the fused one (the split rung, counted), the HOST judge's sides as typed rows (a
 * constant side as its text cells) -- and the program's answer after the steps; a verdict group refused on the
 * answer path; the plan read back from its JSON. No caller makes a verdict step yet: these plans are built by hand
 * on the platform's database.
 */
final class ProgramRunnerTest {

    private static Connection conn;
    private static ExecutionPlan.Target target;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
        target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(Databases.PLATFORM),
                new ExecutionPlan.Servers.Every(), List.of(), List.of());
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static PlanSessions.Source sessions() {
        return PlanSessions.setUp(TestSessions.over(conn), target);
    }

    /** The test-is-the-product plan's M10a: a target naming the graph's SYSTEM database opens the session its holder
     *  hands the source for it (a non-closing view), every other target the caller's own; the shared source and a
     *  writing open refuse a System target by name (the runner knows no database). */
    @Test
    void aSystemTargetOpensTheSystemOpenersSession() throws Exception {
        try (Connection other = TestBackends.platform().open()) {
            Sessions.Session body = TestSessions.over(conn);
            Sessions.Session system = TestSessions.over(other);
            ExecutionPlan.Target systemTarget = new ExecutionPlan.Target(
                    new ExecutionPlan.Database.System(Databases.PLATFORM), new ExecutionPlan.Servers.Every(),
                    List.of(), List.of());
            PlanSessions.Source source = PlanSessions.of(body, null, s -> system);
            assertEquals(Sessions.identity(system), Sessions.identity(source.open(systemTarget)),
                    "the System target opens the opener's session");
            assertEquals(Sessions.identity(body), Sessions.identity(source.open(target)),
                    "any other target the caller's own");
            assertEquals(Sessions.identity(body), Sessions.identity(PlanSessions.of(body, null, null).open(systemTarget)),
                    "no opener: the caller's own for every target");
            var refused = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> PlanSessions.shared().open(systemTarget));
            assertTrue(refused.getMessage().contains("system database"), refused.getMessage());
            var refusedWrite = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> PlanSessions.shared().openToChange(systemTarget));
            assertTrue(refusedWrite.getMessage().contains("read-only"), refusedWrite.getMessage());
        }
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
    private static ExecutionPlan.Verdicts group(ExecutionPlan.TdsResult fused) {
        ExecutionPlan.Side left = new ExecutionPlan.Side.Rows(relation("SELECT 3 AS value",
                List.of(col("value", "Integer", "INTEGER"))));
        ExecutionPlan.Side right = new ExecutionPlan.Side.Constant(List.of(col("value", "Integer", null)),
                List.of(List.of("3")), false);
        ExecutionPlan.Verdict first = new ExecutionPlan.Verdict("assertEquals@2", "assertEquals",
                List.of(left, right), wrapper(true, "x", "x"));
        ExecutionPlan.Verdict second = new ExecutionPlan.Verdict("assertEquals@3", "assertEquals",
                List.of(left, right), wrapper(false, "a", "b"));
        return new ExecutionPlan.Verdicts(List.of(first, second), fused, ExecutionPlan.JudgingRules.LITE);
    }

    /** What the program's sink received, in order. */
    private static final class Recording implements PlanRunner.ProgramSink {
        final List<String> events = new ArrayList<>();
        final List<List<Object>> verdictRows = new ArrayList<>();
        final List<List<PlanRunner.Rows>> sides = new ArrayList<>();
        List<ExecutionPlan.Column> answerColumns;
        final List<List<Object>> answerRows = new ArrayList<>();

        @Override
        public void verdict(ExecutionPlan.Verdict verdict, List<Object> row) {
            events.add("verdict " + verdict.name());
            verdictRows.add(row);
        }

        @Override
        public void sides(ExecutionPlan.Verdict verdict, List<PlanRunner.Rows> rows) {
            events.add("sides " + verdict.name());
            sides.add(rows);
        }

        @Override
        public void fused(ExecutionPlan.Verdicts group, com.legend.error.DataError rejected) {
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
    }

    @Test
    @DisplayName("database judging: the fused statement's rows, one per verdict, in the group's order; the sink told it ran")
    void databaseJudgingReadsTheFusedStatement() throws IOException {
        Recording sink = new Recording();
        PlanRunner.program(new ExecutionPlan(List.of(), group(fused(FUSED))), Map.of(), sessions(),
                PlanRunner.Judging.DATABASE, sink);
        assertEquals(List.of("fused", "verdict assertEquals@2", "verdict assertEquals@3"), sink.events);
        assertEquals(java.util.Arrays.asList(true, "x", "x", null, false), cells(sink.verdictRows.get(0)));
        assertEquals(java.util.Arrays.asList(false, "a", "b", null, false), cells(sink.verdictRows.get(1)));
    }

    @Test
    @DisplayName("the split rung: the database rejects the fused statement, the sink told, each verdict's own wrapper runs")
    void aRejectedFusedStatementFallsToEachWrapper() throws IOException {
        Recording sink = new Recording();
        PlanRunner.program(new ExecutionPlan(List.of(), group(fused("SELECT * FROM no_such_table_s1b"))), Map.of(),
                sessions(), PlanRunner.Judging.DATABASE, sink);
        assertEquals(3, sink.events.size(), sink.events.toString());
        assertTrue(sink.events.get(0).startsWith("fused rejected: "), sink.events.get(0));
        assertEquals(List.of("verdict assertEquals@2", "verdict assertEquals@3"), sink.events.subList(1, 3));
        assertEquals(java.util.Arrays.asList(true, "x", "x", null, false), cells(sink.verdictRows.get(0)));
        assertEquals(java.util.Arrays.asList(false, "a", "b", null, false), cells(sink.verdictRows.get(1)));
    }

    @Test
    @DisplayName("no fused statement: each verdict's wrapper runs; a verdict without one is the host judge's: its sides"
            + " read, no row")
    void noFusedStatementRunsEachWrapper() throws IOException {
        ExecutionPlan.Verdicts g = group(null);
        ExecutionPlan.Verdict unjudged = new ExecutionPlan.Verdict("assertIs@4", "assertIs",
                g.verdicts().get(0).base(), null);
        ExecutionPlan.Verdicts withUnjudged = new ExecutionPlan.Verdicts(
                List.of(g.verdicts().get(0), unjudged, g.verdicts().get(1)), null, ExecutionPlan.JudgingRules.LITE);
        Recording sink = new Recording();
        PlanRunner.program(new ExecutionPlan(List.of(), withUnjudged), Map.of(), sessions(),
                PlanRunner.Judging.DATABASE, sink);
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
        PlanRunner.program(new ExecutionPlan(List.of(), group(fused(FUSED))), Map.of(), sessions(),
                PlanRunner.Judging.HOST, sink);
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
        PlanRunner.program(new ExecutionPlan(List.of(), group(fused(FUSED))), Map.of(), sessions(),
                PlanRunner.Judging.BOTH, sink);
        assertEquals(List.of("sides assertEquals@2", "sides assertEquals@3", "fused", "verdict assertEquals@2",
                "verdict assertEquals@3"), sink.events);
    }

    @Test
    @DisplayName("a program with an answer: the verdicts, then the result step's rows after the steps")
    void aProgramsAnswerFollowsItsVerdicts() throws IOException {
        ExecutionPlan.Sequence body = new ExecutionPlan.Sequence(List.of(
                new ExecutionPlan.Check(ExecutionPlan.Sql.of("SELECT 1", target)),
                group(fused(FUSED)),
                relation("SELECT 7 AS value", List.of(col("value", "Integer", "INTEGER")))));
        Recording sink = new Recording();
        PlanRunner.program(new ExecutionPlan(List.of(), body), Map.of(), sessions(), PlanRunner.Judging.DATABASE,
                sink);
        assertEquals(List.of("fused", "verdict assertEquals@2", "verdict assertEquals@3", "columns", "batch", "end"),
                sink.events);
        assertEquals(List.of(List.of(7L)), sink.answerRows);
        assertEquals("value", sink.answerColumns.get(0).name());
    }

    @Test
    @DisplayName("a program may answer no rows: a body ending in an assert")
    void aProgramMayAnswerNoRows() throws IOException {
        Recording sink = new Recording();
        PlanRunner.program(new ExecutionPlan(List.of(), new ExecutionPlan.Sequence(List.of(
                new ExecutionPlan.Check(ExecutionPlan.Sql.of("SELECT 1", target)), group(fused(FUSED))))),
                Map.of(), sessions(), PlanRunner.Judging.DATABASE, sink);
        assertEquals(List.of("fused", "verdict assertEquals@2", "verdict assertEquals@3"), sink.events);
        assertNull(sink.answerColumns);
    }

    @Test
    @DisplayName("a verdict group is a program's step: refused on the answer path, as a root and inside a sequence")
    void aVerdictGroupIsRefusedOnTheAnswerPath() {
        ExecutionPlan root = new ExecutionPlan(List.of(), group(fused(FUSED)));
        Recording sink = new Recording();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> PlanRunner.stream(root, Map.of(), sessions(), sink));
        assertTrue(e.getMessage().contains("program(...)"), e.getMessage());
        ExecutionPlan inSequence = new ExecutionPlan(List.of(), new ExecutionPlan.Sequence(List.of(group(fused(FUSED)),
                relation("SELECT 7 AS value", List.of(col("value", "Integer", "INTEGER"))))));
        IllegalStateException f = assertThrows(IllegalStateException.class,
                () -> PlanRunner.stream(inSequence, Map.of(), sessions(), sink));
        assertTrue(f.getMessage().contains("program(...)"), f.getMessage());
        assertThrows(IllegalStateException.class, root::answerShape, "a plan that only judges answers no rows");
    }

    @Test
    @DisplayName("the plan reads back from its JSON: every side kind, a wrapper, a fused statement, no wrapper")
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
        ExecutionPlan.Side keyedSide = new ExecutionPlan.Side.KeyedRows(keyed);
        ExecutionPlan.Verdicts g = group(fused(FUSED));
        ExecutionPlan.Verdict third = new ExecutionPlan.Verdict("assertTdsEquivalent@5", "assertTdsEquivalent",
                List.of(keyedSide, new ExecutionPlan.Side.Constant(List.of(col("a", "Integer", null),
                        optional("b", "String", null)), List.of(List.of("1", "x"), java.util.Arrays.asList("2", null)),
                        true)),
                null);
        ExecutionPlan plan = new ExecutionPlan(List.of(), new ExecutionPlan.Sequence(List.of(
                new ExecutionPlan.Check(ExecutionPlan.Sql.of("SELECT 1", target)),
                new ExecutionPlan.Verdicts(List.of(g.verdicts().get(0), g.verdicts().get(1), third), g.fused(),
                        ExecutionPlan.JudgingRules.LITE),
                relation("SELECT 7 AS value", List.of(col("value", "Integer", "INTEGER"))))));
        String json = PlanJson.write(plan);
        assertEquals(plan, PlanJson.read(json), "the plan read back differs");
        assertTrue(json.contains("\"_type\":\"verdicts\"") && json.contains("\"_type\":\"keyedRows\"")
                && json.contains("\"_type\":\"constant\"") && json.contains("\"folded\":true"), json);
    }

    // ---- a branch (the asserts-as-plan-nodes program, step 4, 2026-11-04): an if whose sides hold asserts ----------

    /** One assert whose one rows side reads {@code sql}. */
    private static ExecutionPlan.Verdicts oneVerdict(String name, String sql) {
        ExecutionPlan.Side side = new ExecutionPlan.Side.Rows(relation(sql, List.of(col("value", "Integer", "INTEGER"))));
        return new ExecutionPlan.Verdicts(List.of(new ExecutionPlan.Verdict(name, "assert", List.of(side), null)),
                null, ExecutionPlan.JudgingRules.LITE);
    }

    @Test
    @DisplayName("a branch: the condition read, the side it names run, the other never sent")
    void aBranchRunsTheSideItsConditionNames() throws IOException {
        ExecutionPlan.Side condition = new ExecutionPlan.Side.Rows(relation("SELECT 1 > 2 AS value",
                List.of(col("value", "Boolean", "BOOLEAN"))));
        // the side not taken reads a table that does not exist: sent, it would fail the program
        ExecutionPlan.Branch branch = new ExecutionPlan.Branch(condition,
                List.of(oneVerdict("then@1", "SELECT v AS value FROM no_such_table_branch")),
                List.of(oneVerdict("else@1", "SELECT 4 AS value")));
        Recording sink = new Recording();
        PlanRunner.program(new ExecutionPlan(List.of(), new ExecutionPlan.Sequence(List.of(branch))), Map.of(),
                sessions(), PlanRunner.Judging.HOST, sink);
        assertEquals(List.of("sides else@1"), sink.events);
        assertEquals(List.of(4L), sink.sides.get(0).get(0).rows().get(0));
    }

    @Test
    @DisplayName("a branch that sends nothing: its constant condition read from the program's text, no session opened")
    void aConstantBranchRunsWithNoSession() throws IOException {
        ExecutionPlan.Side yes = new ExecutionPlan.Side.Constant(List.of(col("value", "Boolean", null)),
                List.of(List.of("true")), false);
        ExecutionPlan.Side three = new ExecutionPlan.Side.Constant(List.of(col("value", "Integer", null)),
                List.of(List.of("3")), false);
        ExecutionPlan.Verdicts then = new ExecutionPlan.Verdicts(List.of(new ExecutionPlan.Verdict("then@1",
                "assertEquals", List.of(three, three), null)), null, ExecutionPlan.JudgingRules.LITE);
        ExecutionPlan.Verdicts otherwise = new ExecutionPlan.Verdicts(List.of(new ExecutionPlan.Verdict("else@1",
                "assertEquals", List.of(three, three), null)), null, ExecutionPlan.JudgingRules.LITE);
        ExecutionPlan.Branch inner = new ExecutionPlan.Branch(yes, List.of(), List.of(otherwise));
        ExecutionPlan.Branch branch = new ExecutionPlan.Branch(yes, List.of(then, inner), List.of(otherwise));
        Recording sink = new Recording();
        PlanRunner.program(new ExecutionPlan(List.of(), branch), Map.of(), PlanSessions.shared(),
                PlanRunner.Judging.BOTH, sink);
        assertEquals(List.of("sides then@1", "verdict then@1"), sink.events);
    }

    @Test
    @DisplayName("a program of constants alone under the database judge hands a host-judged verdict its sides")
    void aConstantHostVerdictGetsItsSidesUnderTheDatabaseJudge() throws IOException {
        // an assertError's step is the host's in both modes (ruling N3): with no wrapper and constant sides, the runner's
        // constants-alone path handed it no sides until 2026-11-04, so it was never judged under the database judge
        ExecutionPlan.Side three = new ExecutionPlan.Side.Constant(List.of(col("value", "Integer", null)),
                List.of(List.of("3")), false);
        ExecutionPlan.Verdicts step = new ExecutionPlan.Verdicts(List.of(new ExecutionPlan.Verdict("assertError@1",
                "assertError", List.of(three), null)), null, ExecutionPlan.JudgingRules.LITE);
        Recording sink = new Recording();
        PlanRunner.program(new ExecutionPlan(List.of(), step), Map.of(), PlanSessions.shared(),
                PlanRunner.Judging.DATABASE, sink);
        assertEquals(List.of("sides assertError@1", "verdict assertError@1"), sink.events);
    }

    @Test
    @DisplayName("a branch reads back from its JSON, and its condition is no answer")
    void aBranchReadsBackFromItsJson() {
        ExecutionPlan.Side condition = new ExecutionPlan.Side.Rows(relation("SELECT TRUE AS value",
                List.of(col("value", "Boolean", "BOOLEAN"))));
        ExecutionPlan.Branch inner = new ExecutionPlan.Branch(new ExecutionPlan.Side.Constant(
                List.of(col("value", "Boolean", null)), List.of(List.of("false")), false),
                List.of(group(fused(FUSED))), List.of());
        ExecutionPlan plan = new ExecutionPlan(List.of(), new ExecutionPlan.Sequence(List.of(
                new ExecutionPlan.Branch(condition, List.of(oneVerdict("then@1", "SELECT 1 AS value"), inner),
                        List.of(oneVerdict("else@1", "SELECT 2 AS value"))))));
        String json = PlanJson.write(plan);
        assertEquals(plan, PlanJson.read(json), "the plan read back differs");
        assertTrue(json.contains("\"_type\":\"branch\""), json);
        // the rules each verdict group is judged by are the plan's own (the explicit rule, 2026-11-04)
        assertTrue(json.contains("\"rules\":\"lite\""), json);
        assertThrows(IllegalStateException.class, plan::answerShape, "a plan that only branches answers no rows");
        assertThrows(IllegalArgumentException.class, () -> new ExecutionPlan.Branch(condition,
                List.of(relation("SELECT 1 AS value", List.of(col("value", "Integer", "INTEGER")))), List.of()),
                "a branch's side holds verdict groups and branches");
    }

    private static List<Object> cells(List<Object> row) {
        return new ArrayList<>(row);
    }

    // ---- M3b (the test-is-the-product plan's B12): an effect's script, a seed step, a verdict group's savepoint ----

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

    private static List<String> column(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (java.sql.Statement st = conn.createStatement(); java.sql.ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    /** {@code statements} as one effect step, scripted in the platform dialect's bracket. */
    private static ExecutionPlan.Effect script(List<String> statements) {
        com.legend.sql.dialect.SqlDialect dialect = Databases.dialect(Databases.PLATFORM);
        return new ExecutionPlan.Effect(ExecutionPlan.Sql.of(String.join(";\n", statements) + ";", target),
                new ExecutionPlan.Effect.Script(statements, dialect.script(statements), dialect.scriptAbort()));
    }

    private static Recording run(ExecutionPlan.Node root, PlanRunner.Judging judging) throws IOException {
        ExecutionPlan plan = new ExecutionPlan(List.of(), root);
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)), "the plan reads back as written");
        Recording sink = new Recording();
        PlanRunner.program(plan, Map.of(), sessions(), judging, sink);
        return sink;
    }

    @Test
    @DisplayName("an effect script runs in its dialect's bracket; a failure aborts the bracket and applies nothing")
    void anEffectScriptRunsBracketedAndAFailureAppliesNothing() throws Exception {
        run(script(List.of("CREATE OR REPLACE TABLE m3b_s (a INTEGER)", "INSERT INTO m3b_s VALUES (1)")),
                PlanRunner.Judging.HOST);
        assertEquals(1, count("m3b_s"));
        // the second statement fails at run time: DuckDB's bracket applies neither, and the abort closes it
        assertThrows(com.legend.error.DataError.class, () -> run(script(List.of("INSERT INTO m3b_s VALUES (2)",
                "INSERT INTO m3b_s VALUES ('not a number')")), PlanRunner.Judging.HOST));
        assertEquals(1, count("m3b_s"), "the failed script applied nothing, and the session is usable");
    }

    @Test
    @DisplayName("a seed step runs once on a session, is skipped while the session holds its seeds, re-runs after a write")
    void aSeedStepRunsOnceUntilAWrite() throws Exception {
        ExecutionPlan.Setup seeds = new ExecutionPlan.Setup("m3b-seeds", List.of(new ExecutionPlan.SetupStep.Statement(
                "CREATE OR REPLACE TABLE m3b_seed AS SELECT 1 AS a")), target);
        ExecutionPlan.TdsResult read = relation("SELECT count(*) AS n FROM m3b_seed",
                List.of(col("n", "Integer", "BIGINT")));
        run(new ExecutionPlan.Sequence(List.of(seeds, read)), PlanRunner.Judging.HOST);
        assertEquals(1, count("m3b_seed"));
        exec("INSERT INTO m3b_seed VALUES (2)");   // outside the runner: the session's memo does not see it
        run(new ExecutionPlan.Sequence(List.of(seeds, read)), PlanRunner.Judging.HOST);
        assertEquals(2, count("m3b_seed"), "skipped: the session holds the seeds and ran no write");
        run(new ExecutionPlan.Sequence(List.of(new ExecutionPlan.Effect(ExecutionPlan.Sql.of(
                "INSERT INTO m3b_seed VALUES (3)", target)), seeds, read)), PlanRunner.Judging.HOST);
        assertEquals(1, count("m3b_seed"), "re-seeded after the program's write");
    }

    @Test
    @DisplayName("F11: a fused statement inside a transaction is guarded by the group's savepoint, outside it is not")
    void aFusedStatementInATransactionIsGuardedByItsSavepoint() throws Exception {
        // the savepoint's three texts are stand-ins that log their step (DuckDB has no savepoint; a catalog error
        // does not abort its transaction, so the rejected fused statement leaves the rung observable)
        exec("CREATE OR REPLACE TABLE m3b_sp (step VARCHAR)");
        exec("CREATE OR REPLACE TABLE m3b_w (a INTEGER)");
        ExecutionPlan.Verdicts.Savepoint sp = new ExecutionPlan.Verdicts.Savepoint(
                "INSERT INTO m3b_sp VALUES ('set')", "INSERT INTO m3b_sp VALUES ('rollbackTo')",
                "INSERT INTO m3b_sp VALUES ('release')");
        ExecutionPlan.Update write = new ExecutionPlan.Update(ExecutionPlan.Sql.of("INSERT INTO m3b_w VALUES (1)",
                target), null);
        ExecutionPlan.Verdicts held = group(fused(FUSED));
        ExecutionPlan.Verdicts guarded = new ExecutionPlan.Verdicts(held.verdicts(), held.fused(), sp, held.rules());
        run(new ExecutionPlan.Sequence(List.of(write, guarded)), PlanRunner.Judging.DATABASE);
        assertEquals(List.of("set", "release"), column("SELECT step FROM m3b_sp ORDER BY rowid"));
        ExecutionPlan.Verdicts rejected = group(fused("SELECT * FROM no_such_table_m3b"));
        Recording split = run(new ExecutionPlan.Sequence(List.of(write, new ExecutionPlan.Verdicts(
                rejected.verdicts(), rejected.fused(), sp, rejected.rules()))), PlanRunner.Judging.DATABASE);
        assertEquals(List.of("set", "release", "set", "rollbackTo"), column("SELECT step FROM m3b_sp ORDER BY rowid"));
        assertEquals(2, split.verdictRows.size(), "the wrappers ran after the rollback to the savepoint");
        // outside any transaction (no write, the session's own commits): no savepoint statement is sent
        run(guarded, PlanRunner.Judging.DATABASE);
        assertEquals(4, count("m3b_sp"));
    }

    // ---- the M3 audit's findings (2026-11-01) ----

    /** B2: a keyed step's snapshot holds the program in a transaction, so an effect's script is sent bare inside it
     *  (asked at each step), never its BEGIN ... COMMIT bracket within the open transaction. */
    @Test
    @DisplayName("an effect before a keyed step runs bare inside the keyed step's snapshot transaction")
    void anEffectBeforeAKeyedStepRunsInsideItsSnapshot() throws Exception {
        ExecutionPlan.Sql keys = ExecutionPlan.Sql.of("SELECT 'k' AS k, 'k__|__v' AS n", target);
        String bodyText = "SELECT 1 AS a, ? AS _pk0_v";
        ExecutionPlan.Sql keyedBody = new ExecutionPlan.Sql(bodyText,
                List.of(new ExecutionPlan.Slot("", new ExecutionPlan.Binding.Key(0))), target, null,
                List.of(new ExecutionPlan.Region(13, bodyText.length(), "", ", ", "", 0, 1, List.of())));
        ExecutionPlan.Keyed keyed = new ExecutionPlan.Keyed(keys, List.of("k", "n"),
                List.of(new ExecutionPlan.Keyed.Fixed(col("a", "Integer", "INTEGER"), "a"),
                        new ExecutionPlan.Keyed.PerKey(col("v", "String", "VARCHAR"), 1, "v")),
                1, keyedBody, ExecutionPlan.Keyed.Snapshot.TRANSACTION, null);
        run(new ExecutionPlan.Sequence(List.of(script(List.of("CREATE OR REPLACE TABLE m3x_k (a INTEGER)",
                "INSERT INTO m3x_k VALUES (1)")), keyed)), PlanRunner.Judging.HOST);
        assertEquals(1, count("m3x_k"));
    }

    /** B3: a seed step carries the program's target, so seeds before a constant answer open the session. */
    @Test
    @DisplayName("seeds before a constant answer: the seed step opens the program's session")
    void seedsBeforeAConstantOpenTheSession() throws Exception {
        ExecutionPlan.Setup seeds = new ExecutionPlan.Setup("m3x-seeds", List.of(new ExecutionPlan.SetupStep.Statement(
                "CREATE OR REPLACE TABLE m3x_c AS SELECT 1 AS a")), target);
        ExecutionPlan.Constant answer = new ExecutionPlan.Constant(List.of(new ExecutionPlan.Column("value", "String",
                new ExecutionPlan.Multiplicity(1, 1), null, null, null)), List.of(List.of("x")));
        Recording sink = run(new ExecutionPlan.Sequence(List.of(seeds, answer)), PlanRunner.Judging.HOST);
        assertEquals(List.of(List.of("x")), sink.answerRows);
        assertEquals(1, count("m3x_c"));
    }

    /** B1: a rollback may undo seeds run inside the transaction, so the next program's seed step runs again. */
    @Test
    @DisplayName("a seed step undone by a rollback runs again in the next program")
    void aSeedUndoneByARollbackRunsAgain() throws Exception {
        exec("DROP TABLE IF EXISTS m3x_r");
        ExecutionPlan.Setup seeds = new ExecutionPlan.Setup("m3x-rollback", List.of(new ExecutionPlan.SetupStep.Statement(
                "CREATE TABLE m3x_r AS SELECT 1 AS a")), target);
        ExecutionPlan.Update failing = new ExecutionPlan.Update(ExecutionPlan.Sql.of(
                "INSERT INTO no_such_table_m3x VALUES (1)", target), null);
        assertThrows(RuntimeException.class, () -> run(new ExecutionPlan.Sequence(List.of(seeds, failing)),
                PlanRunner.Judging.HOST));
        ExecutionPlan.TdsResult read = relation("SELECT count(*) AS n FROM m3x_r", List.of(col("n", "Integer",
                "BIGINT")));
        Recording sink = run(new ExecutionPlan.Sequence(List.of(seeds, read)), PlanRunner.Judging.HOST);
        assertEquals(1L, ((Number) sink.answerRows.get(0).get(0)).longValue());
    }
}
