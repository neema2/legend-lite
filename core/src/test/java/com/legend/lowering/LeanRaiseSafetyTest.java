// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E-V2'S REVIEW, THE LOWERING'S HALF (2026-11-16): each lean rule keeps every raise and every column it must, pinned on
 * the MIR so a revert of the guard turns it red, beside a control showing the rule still fires where it may.
 */
class LeanRaiseSafetyTest {

    private static final OutputCol A = new OutputCol("A", SqlType.Scalar.BIGINT, false);
    private static final OutputCol B = new OutputCol("B", SqlType.Scalar.BIGINT, false);
    private static final OutputCol S = new OutputCol("S", SqlType.Scalar.VARCHAR, false);
    private static final SqlSource.Table T = new SqlSource.Table("T", "t0", List.of(A, B, S), false, Map.of(),
            NameQuoting.BARE);

    private static SqlExpr a() {
        return SqlExpr.Column.of("t0", A);
    }

    private static SqlExpr b() {
        return SqlExpr.Column.of("t0", B);
    }

    /** ((A * 3 + 1) * (B + 7) - A): a repeat worth binding when read three times. */
    private static SqlExpr e() {
        return SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.TIMES,
                SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.TIMES, a(), new SqlExpr.IntLit(3)),
                        new SqlExpr.IntLit(1)),
                SqlExpr.Call.of(SqlFn.PLUS, b(), new SqlExpr.IntLit(7))), a());
    }

    private static SqlSelect select(List<SqlSelect.Projection> ps) {
        return new SqlSelect(ps, false, T, null, List.of(), null, null, List.of(), null, null,
                ps.stream().map(p -> p.out() == null ? A : p.out()).toList());
    }

    private static boolean bindsRowValues(SqlQuery q) {
        return q instanceof SqlSelect s && s.from() instanceof SqlSource.RowValues;
    }

    @Test
    void aRowsRepeatIsBound_butNotBesideAStar() {
        OutputCol sq = new OutputCol("sq", SqlType.Scalar.BIGINT, false);
        SqlExpr value = SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.TIMES, e(), e()), e());
        // control: the repeat is bound
        assertTrue(bindsRowValues(LetBinding.bindRowRepeats(select(List.of(new SqlSelect.Projection(value, "sq",
                sq))))));
        // B-M1: a star would read the bound value as a column of its own: the select keeps its terms
        SqlSelect starred = select(List.of(new SqlSelect.Projection(new SqlExpr.Star(null), null, null),
                new SqlSelect.Projection(value, "sq", sq)));
        assertFalse(bindsRowValues(LetBinding.bindRowRepeats(starred)));
    }

    private static SqlSelect countOver(SqlSelect rows) {
        OutputCol n = new OutputCol("value", SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(SqlAgg.Reducer.of(SqlAgg.Fn.COUNT), "value", n)),
                false, new SqlSource.Subselect(rows, "t1", null), null, List.of(), null, null, List.of(), null, null,
                List.of(n));
    }

    @Test
    void aCountDropsItsRowsOrder_unlessAKeyCanRaise() {
        SqlSelect sortedByA = select(List.of(new SqlSelect.Projection(a(), "A", A)))
                .withOrderBy(List.of(new SqlSelect.SortKey(a(), true, null, null)));
        // control: the wrapper goes
        assertInstanceOf(SqlSource.Table.class, ((SqlSelect) CountCollapse.apply(countOver(sortedByA))).from());
        // A-M2: a key that can raise (text read as an integer) keeps it
        SqlSelect sortedByCast = select(List.of(new SqlSelect.Projection(a(), "A", A))).withOrderBy(List.of(
                new SqlSelect.SortKey(new SqlExpr.Cast(SqlExpr.Column.of("t0", S), SqlType.Scalar.INTEGER,
                        SqlExpr.Reason.USER), true, null, null)));
        assertInstanceOf(SqlSource.Subselect.class, ((SqlSelect) CountCollapse.apply(countOver(sortedByCast))).from());
    }

    @Test
    void aLiteralsFieldIsReadOffIt_unlessAFieldCanRaise() {
        SqlExpr.StructLit plain = new SqlExpr.StructLit(List.of(new SqlExpr.StructLit.Field("__id",
                new SqlExpr.StringLit("o1")), new SqlExpr.StructLit.Field("name", new SqlExpr.StringLit("n"))));
        // control: the id is the literal's own value
        assertInstanceOf(SqlExpr.StringLit.class, CanonicalRenderSql.fieldOf(plain, "__id"));
        // A-S1: a field that raises at construction is evaluated: the read goes through the struct
        SqlExpr.StructLit raising = new SqlExpr.StructLit(List.of(new SqlExpr.StructLit.Field("__id",
                new SqlExpr.StringLit("o1")), new SqlExpr.StructLit.Field("name", new SqlExpr.Cast(
                        new SqlExpr.StringLit("x"), SqlType.Scalar.INTEGER, SqlExpr.Reason.USER))));
        assertInstanceOf(SqlExpr.StructGet.class, CanonicalRenderSql.fieldOf(raising, "__id"));
    }
}
