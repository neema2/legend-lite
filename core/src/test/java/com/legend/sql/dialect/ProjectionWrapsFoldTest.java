// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/** H2's fold of a select that only names its subselect's columns (2026-11-01: H2 plans each nested derived table again
 *  at every level above it, and a #TS answer's renames ran it out of memory). */
class ProjectionWrapsFoldTest {

    private static final SqlSource.Table PRICES = new SqlSource.Table("PRICES", "t0", List.of(), false,
            java.util.Map.of(), com.legend.sql.NameQuoting.BARE);

    /** A column of the table ({@code t0}) or of the subselect over it. */
    private static SqlExpr.Column ref(String table, String name) {
        return "t0".equals(table) ? SqlExpr.Column.physical(table, name) : SqlExpr.Column.derived(table, name);
    }

    private static SqlSelect.Projection col(String table, String name, String alias) {
        return new SqlSelect.Projection(ref(table, name), alias, null);
    }

    /** {@code SELECT SYMBOL, CLOSE FROM PRICES WHERE CLOSE > 1}. */
    private static SqlSelect inner() {
        return SqlSelect.starOf(PRICES).withProjections(List.of(col("t0", "SYMBOL", null), col("t0", "CLOSE", null)))
                .withWhere(SqlExpr.Call.of(SqlFn.GREATER, ref("t0", "CLOSE"),
                        ref("t0", "CLOSE")));
    }

    private static SqlSelect wrap(SqlSelect inner) {
        return SqlSelect.starOf(new SqlSource.Subselect(inner, "t1", null))
                .withProjections(List.of(col("t1", "SYMBOL", "S"), col("t1", "CLOSE", null)));
    }

    @Test
    void aRenameOfASubselectsColumnsIsThatSubselectUnderTheNewNames() {
        SqlQuery out = new ProjectionWrapsFold(java.util.function.UnaryOperator.identity()).rewriteRoot(wrap(inner()));
        SqlSelect s = assertInstanceOf(SqlSelect.class, out);
        assertSame(PRICES, s.from(), "one level: the subselect's own source");
        assertEquals(inner().where(), s.where(), "its condition kept");
        assertEquals(List.of("S", "CLOSE"), s.projections().stream().map(SqlSelect.Projection::outputName).toList());
        assertEquals(ref("t0", "SYMBOL"), s.projections().get(0).expr(), "the subselect's expression");
    }

    @Test
    void aWrapThatFiltersOrDedupsOrASubselectThatOrdersOrCutsIsKept() {
        SqlSelect filtered = wrap(inner()).withWhere(SqlExpr.Call.of(SqlFn.GREATER,
                ref("t1", "CLOSE"), ref("t1", "CLOSE")));
        assertSame(filtered, new ProjectionWrapsFold(java.util.function.UnaryOperator.identity()).rewriteRoot(filtered), "a filter over the subselect's rows");
        SqlSelect deduped = wrap(inner().withDistinct());
        assertSame(deduped, new ProjectionWrapsFold(java.util.function.UnaryOperator.identity()).rewriteRoot(deduped), "a narrowed DISTINCT is other rows");
        SqlSelect cut = wrap(new SqlSelect(inner().projections(), false, PRICES, null, List.of(), null, null, List.of(),
                5L, null, List.of()));
        assertSame(cut, new ProjectionWrapsFold(java.util.function.UnaryOperator.identity()).rewriteRoot(cut), "a LIMIT's rows are the subselect's own");
    }

    @Test
    void aComputedValueReadTwiceOrAnUnreadRowMakerIsKept() {
        // the final review of PR #81 (2026-11-09): SelectMerge's rules -- a computed value read twice would be written
        // twice (a random draw drawn twice), and an unread UNNEST dropped would leave its source's rows unexploded
        SqlExpr drawn = SqlExpr.Call.of(SqlFn.RANDOM);
        SqlSelect randomInner = SqlSelect.starOf(PRICES).withProjections(List.of(
                new SqlSelect.Projection(drawn, "R", null), col("t0", "CLOSE", null)));
        SqlSelect readTwice = SqlSelect.starOf(new SqlSource.Subselect(randomInner, "t1", null))
                .withProjections(List.of(col("t1", "R", "R"), col("t1", "R", "R2")));
        assertSame(readTwice, new ProjectionWrapsFold(java.util.function.UnaryOperator.identity()).rewriteRoot(readTwice), "a draw read twice");
        SqlSelect flatInner = SqlSelect.starOf(PRICES).withProjections(List.of(col("t0", "SYMBOL", null),
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.UNNEST, ref("t0", "CLOSE")), "E", null)));
        SqlSelect unread = SqlSelect.starOf(new SqlSource.Subselect(flatInner, "t1", null))
                .withProjections(List.of(col("t1", "SYMBOL", null)));
        assertSame(unread, new ProjectionWrapsFold(java.util.function.UnaryOperator.identity()).rewriteRoot(unread), "an unread UNNEST keeps its rows");
    }

    @Test
    void h2WritesTheWrapAsOneSelect() {
        String sql = new H2().render(wrap(inner()));
        assertEquals(1, sql.split("SELECT", -1).length - 1, sql);
    }
}
