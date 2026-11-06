// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A chain of windowed levels laid out again, each value once at its lowest level ({@link SelectLayers}, 2026-11-04,
 * #169): two words' identical windows computed once, a ranking function's arguments read as any value's, a column of
 * the bottom's source carried up, a value that may raise kept, and a chain of row values alone left to the lowering.
 * Each answer is DuckDB's own, against the query written by hand.
 */
class SelectLayersTest {

    private static final SqlSource.Table PRICES = new SqlSource.Table("PRICES", "t0", List.of(), false,
            java.util.Map.of(), com.legend.sql.NameQuoting.BARE);

    private static SqlExpr.Column col(String table, String name) {
        return "t0".equals(table) ? SqlExpr.Column.physical(table, name) : SqlExpr.Column.derived(table, name);
    }

    private static SqlSelect.Projection as(SqlExpr e, String name) {
        return new SqlSelect.Projection(e, name, null);
    }

    /** {@code fn(arg) OVER (PARTITION BY SYMBOL ORDER BY DAY)} over {@code table}. */
    private static SqlExpr over(String table, SqlAgg fn) {
        return new SqlExpr.WindowCall(fn, List.of(col(table, "SYMBOL")),
                List.of(new SqlSelect.SortKey(col(table, "DAY"), true, null, null)), null);
    }

    private static SqlAgg avg(SqlExpr x) {
        return new SqlAgg.Reducer(SqlAgg.Fn.AVG, List.of(x), false, List.of());
    }

    private static SqlSelect level(SqlSource from, SqlSelect.Projection... ps) {
        return SqlSelect.starOf(from).withProjections(List.of(ps));
    }

    private static SqlSource below(SqlSelect s, String alias) {
        return new SqlSource.Subselect(s, alias, null);
    }

    /** The levels of {@code q}, top down. */
    private static List<SqlSelect> levels(SqlQuery q) {
        List<SqlSelect> out = new ArrayList<>();
        for (SqlSelect s = assertInstanceOf(SqlSelect.class, q); ; s = (SqlSelect) ((SqlSource.Subselect) s.from())
                .inner()) {
            out.add(s);
            if (!(s.from() instanceof SqlSource.Subselect)) {
                return out;
            }
        }
    }

    /** Three levels: a running mean of CLOSE, its double, and the same running mean again beside the double. */
    private static SqlSelect twoWords() {
        SqlSelect l1 = level(PRICES, as(col("t0", "SYMBOL"), "SYMBOL"), as(col("t0", "DAY"), "DAY"),
                as(col("t0", "CLOSE"), "CLOSE"), as(over("t0", avg(col("t0", "CLOSE"))), "a"));
        SqlSelect l2 = level(below(l1, "t1"), as(col("t1", "SYMBOL"), "SYMBOL"), as(col("t1", "DAY"), "DAY"),
                as(col("t1", "CLOSE"), "CLOSE"), as(SqlExpr.Call.of(SqlFn.TIMES, col("t1", "a"),
                        new SqlExpr.IntLit(2)), "b"));
        return level(below(l2, "t2"), as(col("t2", "SYMBOL"), "SYMBOL"), as(col("t2", "DAY"), "DAY"),
                as(col("t2", "b"), "b"), as(over("t2", avg(col("t2", "CLOSE"))), "c"));
    }

    @Test
    void twoWordsIdenticalWindowsAreOneWindowAtTheLowestLevel() throws SQLException {
        SqlQuery out = new SelectLayers().rewriteRoot(twoWords());
        List<SqlSelect> levels = levels(out);
        assertEquals(2, levels.size(), "the mean at the first level, the double and the mean read at the second");
        assertEquals(List.of("SYMBOL", "DAY", "b", "c"),
                levels.get(0).projections().stream().map(SqlSelect.Projection::outputName).toList(),
                "the top's columns, in its order, under its names");
        String sql = new DuckDb().render(out);
        assertEquals(1, sql.split("AVG\\(", -1).length - 1, "one window: " + sql);
        assertEquals(answer("SELECT SYMBOL, DAY, AVG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DAY) * 2 AS b,"
                + " AVG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DAY) AS c FROM PRICES"), answer(sql));
    }

    @Test
    void aRankingFunctionsArgumentsAreItsReads() throws SQLException {
        // LAG's arguments are no child of its window's (a ranking function is no expression): read as any value's,
        // the lag one level above the mean its argument doubles
        SqlSelect l1 = level(PRICES, as(col("t0", "SYMBOL"), "SYMBOL"), as(col("t0", "DAY"), "DAY"),
                as(over("t0", avg(col("t0", "CLOSE"))), "a"));
        SqlSelect l2 = level(below(l1, "t1"), as(col("t1", "SYMBOL"), "SYMBOL"), as(col("t1", "DAY"), "DAY"),
                as(SqlExpr.Call.of(SqlFn.TIMES, col("t1", "a"), new SqlExpr.IntLit(2)), "b"));
        SqlSelect l3 = level(below(l2, "t2"), as(col("t2", "SYMBOL"), "SYMBOL"), as(col("t2", "DAY"), "DAY"),
                as(over("t2", new SqlAgg.RankingFn(SqlAgg.Fn.LAG, List.of(col("t2", "b"), new SqlExpr.IntLit(1)))),
                        "l"));
        SqlSelect l4 = level(below(l3, "t3"), as(col("t3", "SYMBOL"), "SYMBOL"), as(col("t3", "DAY"), "DAY"),
                as(SqlExpr.Call.of(SqlFn.PLUS, col("t3", "l"), new SqlExpr.IntLit(1)), "m"));
        assertEquals(3, levels(new SelectLayers().rewriteRoot(l4)).size(),
                "the mean; the lag of its double; the lag's sum, a level above the lag");
        assertEquals(answer("SELECT SYMBOL, DAY, LAG(m * 2, 1) OVER (PARTITION BY SYMBOL ORDER BY DAY) + 1 AS m FROM"
                + " (SELECT SYMBOL, DAY, AVG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DAY) AS m FROM PRICES)"),
                answer(new DuckDb().render(l4)));
    }

    @Test
    void aColumnOfTheSourceIsCarriedUpToItsReaders() throws SQLException {
        // the bottom's CLOSE less its running mean, written as one expression over the source: the mean a value of its
        // own at the first level, the difference at the second -- CLOSE a column of the first level read there, never
        // the source's name out of its scope
        SqlSelect l1 = level(PRICES, as(col("t0", "SYMBOL"), "SYMBOL"), as(col("t0", "DAY"), "DAY"),
                as(SqlExpr.Call.of(SqlFn.MINUS, col("t0", "CLOSE"), over("t0", avg(col("t0", "CLOSE")))), "z"));
        SqlSelect l2 = level(below(l1, "t1"), as(col("t1", "SYMBOL"), "SYMBOL"), as(col("t1", "DAY"), "DAY"),
                as(over("t1", avg(col("t1", "z"))), "zz"));
        SqlSelect l3 = level(below(l2, "t2"), as(col("t2", "SYMBOL"), "SYMBOL"), as(col("t2", "DAY"), "DAY"),
                as(SqlExpr.Call.of(SqlFn.PLUS, col("t2", "zz"), new SqlExpr.IntLit(1)), "r"));
        SqlSelect l4 = level(below(l3, "t3"), as(col("t3", "SYMBOL"), "SYMBOL"), as(col("t3", "DAY"), "DAY"),
                as(col("t3", "r"), "r"));
        SqlQuery out = new SelectLayers().rewriteRoot(l4);
        assertEquals(3, levels(out).size(), "the mean; the difference and its mean; the sum");
        String sql = new DuckDb().render(out);
        assertEquals(answer("SELECT SYMBOL, DAY, AVG(z) OVER (PARTITION BY SYMBOL ORDER BY DAY) + 1 AS r FROM"
                + " (SELECT SYMBOL, DAY, CLOSE - AVG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DAY) AS z FROM PRICES)"),
                answer(sql), sql);
    }

    @Test
    void aChainOfRowValuesAloneIsTheLowerings() {
        // no window: the lowering's fold and SelectMerge lay it out (its budget for a value read twice is theirs)
        SqlSelect l1 = level(PRICES, as(col("t0", "SYMBOL"), "SYMBOL"),
                as(SqlExpr.Call.of(SqlFn.PLUS, col("t0", "CLOSE"), col("t0", "CLOSE")), "x"));
        SqlSelect l2 = level(below(l1, "t1"), as(col("t1", "SYMBOL"), "SYMBOL"),
                as(SqlExpr.Call.of(SqlFn.PLUS, col("t1", "x"), col("t1", "x")), "y"));
        assertSame(l2, new SelectLayers().rewriteRoot(l2));
    }

    @Test
    void aLevelThatFiltersGroupsOrCutsEndsTheChain() {
        SqlSelect l1 = level(PRICES, as(col("t0", "SYMBOL"), "SYMBOL"), as(col("t0", "DAY"), "DAY"),
                as(over("t0", avg(col("t0", "CLOSE"))), "a"));
        SqlSelect cut = new SqlSelect(l1.projections(), false, PRICES, null, List.of(), null, null, List.of(), 5L,
                null, List.of());
        SqlSelect over = level(below(cut, "t1"), as(col("t1", "SYMBOL"), "SYMBOL"),
                as(over("t1", avg(col("t1", "a"))), "aa"));
        assertSame(over, new SelectLayers().rewriteRoot(over), "a LIMIT's rows are its level's own");
        SqlSelect filtered = level(below(l1, "t1"), as(col("t1", "SYMBOL"), "SYMBOL"),
                as(over("t1", avg(col("t1", "a"))), "aa")).withWhere(SqlExpr.Call.of(SqlFn.GREATER, col("t1", "a"),
                        new SqlExpr.IntLit(1)));
        assertSame(filtered, new SelectLayers().rewriteRoot(filtered), "a filter over a window's value is a level");
    }

    @Test
    void aRandomDrawIsAValueOfItsOwnLevel() {
        // the A2' review's finding 1 (2026-11-06): RANDOM() read twice above a window is one draw read twice -- never
        // written into each reader, which would draw twice (the lowering's Fold.nondeterministic, the Cypher island's
        // WITH rand() AS r RETURN r = r): its level ends the chain
        for (SqlFn draw : List.of(SqlFn.RANDOM, SqlFn.GUID)) {
            SqlSelect l1 = level(PRICES, as(col("t0", "SYMBOL"), "SYMBOL"), as(col("t0", "DAY"), "DAY"),
                    as(SqlExpr.Call.of(draw), "r"), as(col("t0", "CLOSE"), "CLOSE"));
            SqlSelect l2 = level(below(l1, "t1"), as(col("t1", "r"), "a"), as(col("t1", "r"), "b"),
                    as(over("t1", avg(col("t1", "CLOSE"))), "s"));
            assertSame(l2, new SelectLayers().rewriteRoot(l2), draw + ": its draw a column of its own level");
        }
    }

    /** {@code sql}'s rows over a small PRICES, sorted, each a line. */
    private static List<String> answer(String sql) throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE PRICES (SYMBOL VARCHAR, DAY DATE, CLOSE DOUBLE)");
            s.execute("INSERT INTO PRICES VALUES ('A', DATE '2024-01-01', 10), ('A', DATE '2024-01-02', 11),"
                    + " ('A', DATE '2024-01-03', 13), ('A', DATE '2024-01-04', 12), ('B', DATE '2024-01-01', 50),"
                    + " ('B', DATE '2024-01-02', 48), ('B', DATE '2024-01-03', 51)");
            List<String> rows = new ArrayList<>();
            try (ResultSet r = s.executeQuery(sql)) {
                int n = r.getMetaData().getColumnCount();
                while (r.next()) {
                    StringBuilder b = new StringBuilder();
                    for (int i = 1; i <= n; i++) {
                        Object o = r.getObject(i);
                        b.append(i > 1 ? "|" : "").append(o instanceof Number x ? String.format(java.util.Locale.ROOT,
                                "%.9f", x.doubleValue()) : String.valueOf(o));
                    }
                    rows.add(b.toString());
                }
            }
            rows.sort(null);
            assertTrue(!rows.isEmpty(), sql);
            return rows;
        }
    }
}
