// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * H2's pass for a lateral that only filters its source by the row before it ({@link CorrelatedFiltersAsJoins}; the
 * user's ruling of 2026-11-25 on the Pure-mode gaps): the filter's correlated conjuncts become an inner join's ON,
 * each inner column read through the subquery's output; a conjunct of the subquery's own stays inside; and a lateral
 * of any other shape comes back as it was, to be refused where it is written. The trees are the shapes the lowering
 * writes for {@code $l->lateral(x | $r->filter(y | $y.k == $x.k))}.
 */
class CorrelatedFiltersAsJoinsTest {

    private static SqlSource.Table table(String name, String alias, String... columns) {
        return new SqlSource.Table(name, alias, java.util.Arrays.stream(columns)
                .map(c -> new OutputCol(c, SqlType.Scalar.BIGINT, true)).toList(), false, Map.of(), NameQuoting.BARE);
    }

    private static final SqlSource.Table L = table("L", "l0", "k", "a");
    private static final SqlSource.Table R = table("R", "r0", "k", "b");

    private static SqlExpr sameKey() {
        return SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.physical("r0", "k"), SqlExpr.Column.physical("l0", "k"));
    }

    /** {@code SELECT * FROM L l0 CROSS JOIN LATERAL (inner) t1}. */
    private static SqlSelect lateralOver(SqlSelect inner) {
        return SqlSelect.starOf(new SqlSource.Join(L, new SqlSource.Subselect(inner, "t1", null),
                SqlSource.Join.Kind.CROSS_LATERAL, null));
    }

    private static SqlSelect rewritten(SqlSelect q) {
        SqlRewriter pass = new H2().passes().stream().filter(CorrelatedFiltersAsJoins.class::isInstance).findFirst()
                .orElseThrow(() -> new AssertionError("H2 has no CorrelatedFiltersAsJoins"));
        return assertInstanceOf(SqlSelect.class, pass.rewrite(q));
    }

    private static SqlSource.Join joinOf(SqlSelect q) {
        return assertInstanceOf(SqlSource.Join.class, q.from());
    }

    @Test
    void aFilterCorrelatedToTheRowBeforeIsAnInnerJoinOnIt() {
        SqlSource.Join j = joinOf(rewritten(lateralOver(SqlSelect.starOf(R).withWhere(sameKey()))));
        assertEquals(SqlSource.Join.Kind.INNER, j.kind());
        SqlSource.Subselect sub = assertInstanceOf(SqlSource.Subselect.class, j.right());
        assertEquals("t1", sub.alias(), "the subquery keeps its alias, so the join's columns are unchanged");
        assertNull(assertInstanceOf(SqlSelect.class, sub.inner()).where(), "the filter left the subquery");
        SqlExpr.Call on = assertInstanceOf(SqlExpr.Call.class, j.on());
        assertEquals(SqlFn.EQUAL, on.fn());
        SqlExpr.Column inner = assertInstanceOf(SqlExpr.Column.class, on.args().get(0));
        SqlExpr.Column outer = assertInstanceOf(SqlExpr.Column.class, on.args().get(1));
        assertEquals("t1.k", inner.table() + "." + inner.name(), "the inner column, through the subquery's output");
        assertEquals("l0.k", outer.table() + "." + outer.name(), "the row before it, as written");
    }

    @Test
    void aConjunctOfTheSubquerysOwnStaysInside() {
        SqlExpr local = SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.physical("r0", "b"), new SqlExpr.IntLit(1));
        SqlSource.Join j = joinOf(rewritten(lateralOver(SqlSelect.starOf(R)
                .withWhere(new SqlExpr.Call(SqlFn.AND, List.of(sameKey(), local))))));
        assertEquals(SqlSource.Join.Kind.INNER, j.kind());
        SqlSelect inner = assertInstanceOf(SqlSelect.class, ((SqlSource.Subselect) j.right()).inner());
        assertEquals(local, inner.where(), "the subquery's own conjunct stays its filter");
        assertEquals(SqlFn.EQUAL, assertInstanceOf(SqlExpr.Call.class, j.on()).fn(), "one conjunct moved");
    }

    @Test
    void anInnerColumnIsReadByTheNameItIsProjectedUnder() {
        SqlSelect inner = SqlSelect.starOf(R).withProjections(List.of(
                new SqlSelect.Projection(SqlExpr.Column.physical("r0", "k"), "key", null))).withWhere(sameKey());
        SqlExpr.Call on = assertInstanceOf(SqlExpr.Call.class, joinOf(rewritten(lateralOver(inner))).on());
        SqlExpr.Column read = assertInstanceOf(SqlExpr.Column.class, on.args().get(0));
        assertEquals("t1.key", read.table() + "." + read.name());
    }

    @Test
    void aLateralOfAnyOtherShapeComesBackAsItWas() {
        SqlSelect filtered = SqlSelect.starOf(R).withWhere(sameKey());
        List<SqlSelect> others = List.of(
                // the first row per row before it: no join answers that
                filtered.withLimit(1L),
                filtered.withDistinct(),
                // a filter on a column the subquery does not output
                SqlSelect.starOf(R).withProjections(List.of(
                        new SqlSelect.Projection(SqlExpr.Column.physical("r0", "b"), "b", null))).withWhere(sameKey()),
                // the row before it read in a projection
                SqlSelect.starOf(R).withProjections(List.of(
                        new SqlSelect.Projection(SqlExpr.Column.physical("l0", "a"), "a", null))).withWhere(sameKey()),
                // no filter at all: another pass's, or none's
                SqlSelect.starOf(R));
        for (SqlSelect inner : others) {
            SqlSelect given = lateralOver(inner);
            SqlSource.Join j = joinOf(rewritten(given));
            assertEquals(SqlSource.Join.Kind.CROSS_LATERAL, j.kind(), inner.toString());
            assertSame(((SqlSource.Join) given.from()).right(), j.right(), "the subquery untouched: " + inner);
        }
    }

    @Test
    void h2RunsItAfterItsOtherLateralPasses() {
        List<String> order = new H2().passes().stream().map(p -> p.getClass().getSimpleName()).toList();
        assertTrue(order.indexOf("ConstantLateralsAsRows") < order.indexOf("LateralExplodeToUnion")
                && order.indexOf("LateralExplodeToUnion") < order.indexOf("CorrelatedFiltersAsJoins"), order.toString());
    }
}
