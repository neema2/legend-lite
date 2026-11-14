// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A count of rows a declared key makes one is 1 ({@link KeyedCounts}, the time model's D2): each rule that carries a
 * key -- a WHERE fixing a key's column, plain columns, a join on the other side's key, a GROUP BY -- and each place it
 * must not reach.
 */
class KeyedCountsTest {

    /** prices read as t0, keyed (symbol, date). */
    private static final Map<String, List<String>> KEYS = Map.of("t0", List.of("symbol", "date"), "t9",
            List.of("symbol", "date"));

    private static SqlExpr col(String table, String name) {
        return SqlExpr.Column.derived(table, name);
    }

    private static SqlSource.Table prices(String alias) {
        return new SqlSource.Table("prices", alias, List.of(), false, Map.of(), com.legend.sql.NameQuoting.BARE);
    }

    private static SqlExpr count(SqlExpr... partition) {
        return new SqlExpr.WindowCall(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()),
                List.of(partition), List.of(), null);
    }

    private static SqlSelect select(SqlSource from, SqlExpr where, SqlExpr... projections) {
        List<SqlSelect.Projection> ps = new java.util.ArrayList<>();
        for (int i = 0; i < projections.length; i++) {
            ps.add(new SqlSelect.Projection(projections[i], "c" + i, null));
        }
        return SqlSelect.starOf(from).withProjections(ps).withWhere(where);
    }

    /** Whether the select's last projection was folded to the literal 1. */
    private static boolean folded(SqlQuery q) {
        SqlSelect s = (SqlSelect) KeyedCounts.fold(q, KEYS);
        return s.projections().get(s.projections().size() - 1).expr() instanceof SqlExpr.IntLit one
                && one.value() == 1;
    }

    private static SqlExpr ko(String table) {
        return SqlExpr.Call.of(SqlFn.EQUAL, col(table, "symbol"), new SqlExpr.StringLit("KO"));
    }

    @Test
    void aCountOverTheWholeKeyIsOne() {
        assertEquals(true, folded(select(prices("t0"), null, col("t0", "date"),
                count(col("t0", "symbol"), col("t0", "date")))));
    }

    @Test
    void aCountOfAFrameWithoutTheRowIsNotFolded() {
        // the A2' review's finding 2 (2026-11-06): over one row a frame that ends a row back counts none of it; a frame
        // holding the row counts it
        SqlExpr.WindowCall.Frame back = new SqlExpr.WindowCall.Frame(SqlExpr.WindowCall.Frame.Kind.ROWS,
                new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding(), SqlExpr.WindowCall.Frame.Bound.Preceding.of(1));
        SqlExpr.WindowCall.Frame toDate = new SqlExpr.WindowCall.Frame(SqlExpr.WindowCall.Frame.Kind.ROWS,
                new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding(), new SqlExpr.WindowCall.Frame.Bound.CurrentRow());
        for (var f : List.of(back, toDate)) {
            SqlExpr counted = new SqlExpr.WindowCall(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()),
                    List.of(col("t0", "symbol"), col("t0", "date")), List.of(), f);
            assertEquals(f == toDate, folded(select(prices("t0"), null, col("t0", "date"), counted)), f.toString());
        }
    }

    @Test
    void aWhereFixingAKeysColumnLeavesTheRestAKey() {
        assertEquals(true, folded(select(prices("t0"), ko("t0"), col("t0", "date"), count(col("t0", "date")))));
        // without the WHERE, one date is every symbol's row of it
        assertEquals(false, folded(select(prices("t0"), null, col("t0", "date"), count(col("t0", "date")))));
        // a WHERE that is no equality to a literal fixes nothing
        assertEquals(false, folded(select(prices("t0"), SqlExpr.Call.of(SqlFn.GREATER, col("t0", "symbol"),
                new SqlExpr.StringLit("KO")), col("t0", "date"), count(col("t0", "date")))));
    }

    @Test
    void aCountOfTheWholeAnswerIsNeverFolded() {
        // toOne()'s guard counts the whole answer (no partition): a key pinned by its WHERE says at most one row, but a
        // database that does not enforce the key may hold two, and upstream checks the size whatever the store
        // declares (the final review of PR #81, 2026-11-09)
        SqlExpr pinned = SqlExpr.Call.of(SqlFn.AND, ko("t0"), SqlExpr.Call.of(SqlFn.EQUAL, col("t0", "date"),
                new SqlExpr.StringLit("2024-01-02")));
        assertEquals(false, folded(select(prices("t0"), pinned, col("t0", "close"), count())));
    }

    @Test
    void aFlattenedSelectHasNoKey() {
        // a select-list UNNEST (flatten) makes rows of each source row: a count over the source's key counts them
        // (the final review of PR #81, 2026-11-09: it had folded to 1)
        SqlExpr unnest = SqlExpr.Call.of(SqlFn.UNNEST, col("t0", "close"));
        SqlSelect flat = select(prices("t0"), null, col("t0", "symbol"), col("t0", "date"), unnest);
        SqlSource.Subselect t1 = new SqlSource.Subselect(flat, "t1", null);
        assertEquals(false, folded(select(t1, null, col("t1", "c2"), count(col("t1", "c0"), col("t1", "c1")))));
        // the same select without the UNNEST keeps the key
        SqlSelect plain = select(prices("t0"), null, col("t0", "symbol"), col("t0", "date"), col("t0", "close"));
        assertEquals(true, folded(select(new SqlSource.Subselect(plain, "t1", null), null, col("t1", "c2"),
                count(col("t1", "c0"), col("t1", "c1")))));
    }

    @Test
    void aKeyIsCarriedThroughPlainColumnsUnderEachName() {
        // the date named twice, as a line's period and its date: a key under either name
        SqlSelect line = select(prices("t0"), ko("t0"), col("t0", "date"), col("t0", "date"), col("t0", "close"));
        SqlSource.Subselect t1 = new SqlSource.Subselect(line, "t1", null);
        assertEquals(true, folded(select(t1, null, col("t1", "c1"), count(col("t1", "c1")))));
        // a computed column is no key
        SqlSelect computed = select(prices("t0"), ko("t0"),
                SqlExpr.Call.of(SqlFn.PLUS, col("t0", "close"), new SqlExpr.IntLit(1)));
        assertEquals(false, folded(select(new SqlSource.Subselect(computed, "t2", null), null,
                count(col("t2", "c0")))));
    }

    @Test
    void aJoinOnTheOtherSidesKeyKeepsThisSidesKey() {
        SqlSource.Subselect a = new SqlSource.Subselect(select(prices("t0"), ko("t0"), col("t0", "date")), "a",
                null);
        SqlSource.Subselect b = new SqlSource.Subselect(select(prices("t9"), ko("t9"), col("t9", "date")), "b",
                null);
        SqlExpr on = SqlExpr.Call.of(SqlFn.EQUAL, col("a", "c0"), col("b", "c0"));
        // the later of two dates the join makes equal is that date
        SqlExpr later = new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                col("a", "c0"), col("b", "c0")), col("a", "c0"))), col("b", "c0"), SqlExpr.Reason.OPERATOR);
        SqlSelect met = select(new SqlSource.Join(a, b, SqlSource.Join.Kind.INNER, on), null, later);
        assertEquals(true, folded(select(new SqlSource.Subselect(met, "m", null), null, count(col("m", "c0")))));
        // a count over the join itself, on either side's date
        assertEquals(true, folded(select(new SqlSource.Join(a, b, SqlSource.Join.Kind.INNER, on), null,
                count(col("b", "c0")))));
        // a join on no key multiplies rows: no key
        SqlSource.Subselect all = new SqlSource.Subselect(select(prices("t9"), null, col("t9", "date")), "b",
                null);
        assertEquals(false, folded(select(new SqlSource.Join(a, all, SqlSource.Join.Kind.INNER,
                SqlExpr.Call.of(SqlFn.EQUAL, col("a", "c0"), col("b", "c0"))), null, count(col("a", "c0")))));
    }

    @Test
    void aLeftJoinKeepsOnlyTheLeftsKey() {
        SqlSource.Subselect a = new SqlSource.Subselect(select(prices("t0"), ko("t0"), col("t0", "date")), "a",
                null);
        SqlSource.Subselect b = new SqlSource.Subselect(select(prices("t9"), ko("t9"), col("t9", "date")), "b",
                null);
        SqlSource.Join left = new SqlSource.Join(a, b, SqlSource.Join.Kind.LEFT,
                SqlExpr.Call.of(SqlFn.EQUAL, col("a", "c0"), col("b", "c0")));
        assertEquals(true, folded(select(left, null, count(col("a", "c0")))));
        // the right's columns may be null where no row met: no key of them, nor an equality
        assertEquals(false, folded(select(left, null, count(col("b", "c0")))));
    }

    @Test
    void aGroupIsUniqueOnItsKeys() {
        SqlSelect grouped = select(prices("t0"), null, col("t0", "symbol"),
                new SqlAgg.Reducer(SqlAgg.Fn.MAX, List.of(col("t0", "close")), false, List.of()))
                .withGroupBy(List.of(col("t0", "symbol")));
        assertEquals(true, folded(select(new SqlSource.Subselect(grouped, "g", null), null, col("g", "c0"),
                count(col("g", "c0")))));
    }

    @Test
    void onlyACountOfRowsIsFolded() {
        // COUNT(x) counts x's values, none where x is null
        SqlExpr countOfX = new SqlExpr.WindowCall(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(col("t0", "close")),
                false, List.of()), List.of(col("t0", "symbol"), col("t0", "date")), List.of(), null);
        assertEquals(false, folded(select(prices("t0"), null, countOfX)));
        // a table of no declared key
        SqlSource.Table plain = new SqlSource.Table("trades", "t5", List.of(), false, Map.of(),
                com.legend.sql.NameQuoting.BARE);
        assertEquals(false, folded(select(plain, null, count(col("t5", "symbol"), col("t5", "date")))));
    }
}
