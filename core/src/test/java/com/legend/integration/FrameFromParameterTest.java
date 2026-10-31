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
 * if written so; an expression the database's to compute, its direction its shape's ({@code -(e)} before the current
 * row, anything else after), checked by the database where upstream checks the numbers it computed; a query's
 * parameter bound like any other. Each answer is the literal frame's, on the lane's database; each refusal a refusal
 * on every lane, where without the check one database answers empty windows.
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
    @DisplayName("arithmetic over a parameter is the database's: (2 - 1) PRECEDING, inside its check")
    void arithmeticIsTheDatabases() throws SQLException {
        assertEquals(TWO_ROWS, sums("|test::frames::window(2)"));
        String sql = generateSql("|test::frames::window(2)");
        assertTrue(sql.contains("ROWS BETWEEN (2 - 1) PRECEDING AND CURRENT ROW"), sql);
        assertTrue(sql.contains("CASE WHEN"), "the check around the window: " + sql);
    }

    @Test
    @DisplayName("a start after the end is refused, in upstream's words, where the database answers empty windows")
    void aStartAfterTheEndIsRefused() {
        // -(0 - 1) is one row AFTER the current row: rows(1, 0), refused by upstream; a negative offset, which one
        // database answers with empty windows (the check refuses it there, in upstream's words) and the others refuse
        // themselves (dialect/ComputedFrameOffsetTest has the check's text): refused on every lane
        assertThrows(Exception.class, () -> sums("|test::frames::window(0)"));
        // rows(-1, -2): one row before to two rows before -- nearly every database answers empty windows
        Exception r = assertThrows(Exception.class, () -> sums("|test::frames::between(1, 2)"));
        assertTrue(chain(r).contains("lower bound of window frame cannot be greater"), chain(r));
        // rows(-2, -0): both literal by then, a literal frame, unchecked
        assertEquals(List.of(10L, 30L, 60L, 100L, 300L), sumsOrNulls("|test::frames::between(2, 0)"));
    }

    @Test
    @DisplayName("a frame whose shapes start after the current row and end at it is refused when planned, naming S129")
    void aShapeAfterThatEndsBeforeIsRefused() {
        // 1 - 2 = -1: upstream reads one row BEFORE; lite reads the shape (after): (1 - 2) FOLLOWING AND CURRENT ROW,
        // which no database parses
        Exception e = assertThrows(Exception.class, () -> generateSql("|test::frames::afterShape(2)"));
        assertTrue(chain(e).contains("S129"), chain(e));
    }

    @Test
    @DisplayName("a negative computed offset is refused, by the database's check or by the database")
    void aNegativeOffsetIsRefused() throws SQLException {
        // one row before to the partition's end
        assertEquals(List.of(60L, 60L, 50L, 300L, 300L), sums("|test::frames::toEnd(1)"));
        // -(-1): written before, its value negative -- no order to check against the unbounded end, the sign's check
        assertThrows(Exception.class, () -> sums("|test::frames::toEnd(0 - 1)"));
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
