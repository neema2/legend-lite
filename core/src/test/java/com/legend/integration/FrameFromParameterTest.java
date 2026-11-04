// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A window frame sized by a parameter (docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md, C1; ruling L2, the user,
 * 2026-10-31): a function's {@code rows($back, 0)} takes its call's argument -- a literal argument a literal frame, as
 * if written so, and so is integer arithmetic over spelled integers, worked out as upstream works out its numbers
 * (docs/TENET_CHARTER.md C6.2b, the user 2026-11-04); a bound still an expression (a query's parameter) is the
 * database's to compute, its direction its shape's ({@code -(e)} before the current row, anything else after),
 * checked by the database where upstream checks the numbers it computed, the parameter bound like any other. Each
 * answer is the literal frame's, on the lane's database; each refusal a refusal on every lane, where without the check
 * one database answers empty windows.
 */
class FrameFromParameterTest extends AbstractDatabaseTest {

    private static final String ROW = "(p:Integer, o:Integer, v:Integer, s:Integer)";

    /** Five rows, two partitions (the window tests' NUMS); each sums {@code v} over the frame the body writes. */
    private static final String NUMS = """
            #TDS
                p, o, v
                0, 1, 10
                0, 2, 20
                0, 3, 30
                1, 1, 100
                1, 2, 200
            #""";

    private static String sum(String frame) {
        return NUMS + "->extend(over(~p, [~o->ascending()], " + frame + "), ~s:{p,w,r|$r.v}:y|$y->plus())";
    }

    private static final String FUNCTIONS = """
            ###Pure
            function test::frames::back(back: Integer[1]): Relation<%1$s>[1]
            {
              %2$s
            }
            function test::frames::window(w: Integer[1]): Relation<%1$s>[1]
            {
              %3$s
            }
            function test::frames::afterShape(w: Integer[1]): Relation<%1$s>[1]
            {
              %4$s
            }
            function test::frames::between(a: Integer[1], b: Integer[1]): Relation<%1$s>[1]
            {
              %5$s
            }
            function test::frames::toEnd(a: Integer[1]): Relation<%1$s>[1]
            {
              %6$s
            }
            """.formatted(ROW, sum("rows($back, 0)"), sum("rows(-($w - 1), 0)"), sum("rows(1 - $w, 0)"),
            sum("rows(-$a, -$b)"), sum("rows(-$a, unbounded())"));

    @BeforeEach
    void setUp() throws SQLException {
        connection = backend().open();   // its rows are the query's own (#TDS): no table to set up
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected String getCompletePureModelWithRuntime() {
        return super.getCompletePureModelWithRuntime() + "\n" + FUNCTIONS;
    }

    private List<Long> sums(String query) throws SQLException {
        var r = executeRelation(query + "->sort([~p->ascending(), ~o->ascending()])");
        int s = -1;
        for (int i = 0; i < r.columns().size(); i++) {
            if (r.columns().get(i).name().equals("s")) {
                s = i;
            }
        }
        int col = s;
        return r.rows().stream().map(row -> row.get(col) == null ? null : ((Number) row.get(col)).longValue()).toList();
    }

    /** The answer of {@code rows(-1, 0)}: each value with the one before it in its partition. */
    private static final List<Long> TWO_ROWS = List.of(10L, 30L, 50L, 100L, 300L);

    @Test
    @DisplayName("a literal argument is a literal frame: the answer and the SQL of rows(-1, 0) written so")
    void aLiteralArgumentIsALiteralFrame() throws SQLException {
        assertEquals(TWO_ROWS, sums("|test::frames::back(-1)"));
        assertEquals(TWO_ROWS, sums("|" + sum("rows(-1, 0)")));
        String sql = generateSql("|test::frames::back(-1)");
        assertTrue(sql.contains("ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"), sql);
        assertFalse(sql.contains("CASE"), "no check around a literal frame: " + sql);
    }

    @Test
    @DisplayName("arithmetic over spelled integers is the number it spells: -(2 - 1) is 1 PRECEDING, unchecked (C6.2b)")
    void spelledArithmeticIsTheLiteralFrame() throws SQLException {
        assertEquals(TWO_ROWS, sums("|test::frames::window(2)"));
        String sql = generateSql("|test::frames::window(2)");
        assertTrue(sql.contains("ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"), sql);
        assertFalse(sql.contains("CASE"), "no check around a literal frame: " + sql);
    }

    @Test
    @DisplayName("arithmetic over a query's parameter is the database's: inside its check, the value bound")
    void arithmeticOverAParameterIsTheDatabases() throws Exception {
        String query = "{n: Integer[1]|" + sum("rows(-($n - 1), 0)") + "->sort([~p->ascending(), ~o->ascending()])}";
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(getCompletePureModelWithRuntime()), query)
                .executionPlan("test::TestRuntime");
        String json = com.legend.executionplan.PlanJson.write(plan);
        assertTrue(json.contains("computedPreceding"), json);
        assertTrue(json.contains("CASE WHEN"), "the check around the window: " + json);
        String csv = PlanCases.write(plan, connection, Map.of("n", 2L), PlanCases.Written.CSV::writer);
        List<String> lines = Arrays.asList(csv.split("\r\n"));
        int s = Arrays.asList(lines.get(0).split(",")).indexOf("s");
        assertEquals(List.of("10", "30", "50", "100", "300"),
                lines.subList(1, lines.size()).stream().map(l -> l.split(",", -1)[s]).toList());
    }

    @Test
    @DisplayName("a start after the end is refused, in upstream's words, where the database answers empty windows")
    void aStartAfterTheEndIsRefused() throws Exception {
        // -(0 - 1) spelled is one row AFTER the current row: rows(1, 0), refused when planned, as upstream refuses it
        Exception w = assertThrows(Exception.class, () -> sums("|test::frames::window(0)"));
        assertTrue(chain(w).contains("lower bound of window frame cannot be greater"), chain(w));
        // rows(-1, -2): one row before to two rows before -- nearly every database answers empty windows
        Exception r = assertThrows(Exception.class, () -> sums("|test::frames::between(1, 2)"));
        assertTrue(chain(r).contains("lower bound of window frame cannot be greater"), chain(r));
        // the same frame from a query's parameters is the database's: its check refuses it, in upstream's words, where
        // a database would answer empty windows (dialect/ComputedFrameOffsetTest has the check's text)
        String query = "{a: Integer[1], b: Integer[1]|" + sum("rows(-$a, -$b)") + "}";
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(getCompletePureModelWithRuntime()), query)
                .executionPlan("test::TestRuntime");
        assertThrows(Exception.class, () -> PlanCases.write(plan, connection, Map.of("a", 1L, "b", 2L),
                PlanCases.Written.CSV::writer));
        // rows(-2, -0): both literal by then, a literal frame, unchecked
        assertEquals(List.of(10L, 30L, 60L, 100L, 300L), sumsOrNulls("|test::frames::between(2, 0)"));
    }

    @Test
    @DisplayName("spelled, 1 - 2 is one row before, as upstream reads it; a parameter's shape after is refused, naming S129")
    void aShapeAfterThatEndsBeforeIsRefused() throws SQLException {
        // 1 - 2 = -1: upstream reads one row BEFORE, and so does lite once the argument is spelled (C6.2b)
        assertEquals(TWO_ROWS, sums("|test::frames::afterShape(2)"));
        // a query's parameter leaves it an expression: lite reads the shape (after), (1 - n) FOLLOWING AND CURRENT
        // ROW, which no database parses -- refused when planned
        String query = "{n: Integer[1]|" + sum("rows(1 - $n, 0)") + "}";
        Exception e = assertThrows(Exception.class, () -> Compiler.query(
                Compiler.compileModel(getCompletePureModelWithRuntime()), query).executionPlan("test::TestRuntime"));
        assertTrue(chain(e).contains("S129"), chain(e));
    }

    @Test
    @DisplayName("a negative computed offset is refused, by the database's check or by the database")
    void aNegativeOffsetIsRefused() throws Exception {
        // one row before to the partition's end
        assertEquals(List.of(60L, 60L, 50L, 300L, 300L), sums("|test::frames::toEnd(1)"));
        // -(0 - 1) spelled is 1: one row after to the end, upstream's reading of the number (C6.2b)
        assertEquals(java.util.Arrays.asList(50L, 30L, null, 200L, null), sumsOrNulls("|test::frames::toEnd(0 - 1)"));
        // -(n) with n = -1 from a query's parameter: written before, its value negative -- no order to check against
        // the unbounded end, the sign's check refuses it
        String query = "{n: Integer[1]|" + sum("rows(-$n, unbounded())") + "}";
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(getCompletePureModelWithRuntime()), query)
                .executionPlan("test::TestRuntime");
        assertThrows(Exception.class, () -> PlanCases.write(plan, connection, Map.of("n", -1L),
                PlanCases.Written.CSV::writer));
    }

    @Test
    @DisplayName("a query's parameter sizes the frame, bound as a parameter of the statement")
    void aQueryParameterIsBound() throws Exception {
        String query = "{n: Integer[1]|" + sum("rows(-$n, 0)") + "->sort([~p->ascending(), ~o->ascending()])}";
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(getCompletePureModelWithRuntime()), query)
                .executionPlan("test::TestRuntime");
        // the plan's JSON carries the computed bound: read back, it writes the same text
        String json = com.legend.executionplan.PlanJson.write(plan);
        assertTrue(json.contains("computedPreceding"), json);
        assertEquals(json, com.legend.executionplan.PlanJson.write(com.legend.executionplan.PlanJson.read(json)));
        String csv = PlanCases.write(plan, connection, Map.of("n", 1L), PlanCases.Written.CSV::writer);
        List<String> lines = Arrays.asList(csv.split("\r\n"));
        int s = Arrays.asList(lines.get(0).split(",")).indexOf("s");
        assertEquals(List.of("10", "30", "50", "100", "300"),
                lines.subList(1, lines.size()).stream().map(l -> l.split(",", -1)[s]).toList());
    }

    private List<Long> sumsOrNulls(String query) {
        try {
            return sums(query);
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    private static String chain(Throwable e) {
        StringBuilder b = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause()) {
            b.append(t.getMessage()).append(" | ");
        }
        return b.toString();
    }
}
