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
                List.of(List.of("3")));
        ExecutionPlan.Verdict first = new ExecutionPlan.Verdict("assertEquals@2", "assertEquals", true,
                List.of(left, right), wrapper(true, "x", "x"));
        ExecutionPlan.Verdict second = new ExecutionPlan.Verdict("assertEquals@3", "assertEquals", true,
                List.of(left, right), wrapper(false, "a", "b"));
        return new ExecutionPlan.Verdicts(List.of(first, second), fused);
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
    @DisplayName("no fused statement: each verdict's wrapper runs; a verdict without one hands the sink no row")
    void noFusedStatementRunsEachWrapper() throws IOException {
        ExecutionPlan.Verdicts g = group(null);
        ExecutionPlan.Verdict unjudged = new ExecutionPlan.Verdict("assertIs@4", "assertIs", true,
                g.verdicts().get(0).base(), null);
        ExecutionPlan.Verdicts withUnjudged = new ExecutionPlan.Verdicts(
                List.of(g.verdicts().get(0), unjudged, g.verdicts().get(1)), null);
        Recording sink = new Recording();
        PlanRunner.program(new ExecutionPlan(List.of(), withUnjudged), Map.of(), sessions(),
                PlanRunner.Judging.DATABASE, sink);
        assertEquals(List.of("verdict assertEquals@2", "verdict assertIs@4", "verdict assertEquals@3"), sink.events);
        assertEquals(java.util.Arrays.asList(true, "x", "x", null, false), cells(sink.verdictRows.get(0)));
        assertNull(sink.verdictRows.get(1), "a verdict with no wrapper: the database judges nothing");
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
        ExecutionPlan.Verdict third = new ExecutionPlan.Verdict("assertTdsEquivalent@5", "assertTdsEquivalent", false,
                List.of(keyedSide, new ExecutionPlan.Side.Constant(List.of(col("a", "Integer", null),
                        optional("b", "String", null)), List.of(List.of("1", "x"), java.util.Arrays.asList("2", null)))),
                null);
        ExecutionPlan plan = new ExecutionPlan(List.of(), new ExecutionPlan.Sequence(List.of(
                new ExecutionPlan.Check(ExecutionPlan.Sql.of("SELECT 1", target)),
                new ExecutionPlan.Verdicts(List.of(g.verdicts().get(0), g.verdicts().get(1), third), g.fused()),
                relation("SELECT 7 AS value", List.of(col("value", "Integer", "INTEGER"))))));
        String json = PlanJson.write(plan);
        assertEquals(plan, PlanJson.read(json), "the plan read back differs");
        assertTrue(json.contains("\"_type\":\"verdicts\"") && json.contains("\"_type\":\"keyedRows\"")
                && json.contains("\"_type\":\"constant\""), json);
    }

    private static List<Object> cells(List<Object> row) {
        return new ArrayList<>(row);
    }
}
