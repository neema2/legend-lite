// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.List;

/**
 * Comparator-lambda recognition for the sort/max/min scalar rules —
 * split from {@link Scalars} (which owns rule REGISTRATION) purely by
 * size. Same doctrine: recognized comparator shapes lower; valid pure
 * shapes beyond them wall via NotImplementedException, never ISE.
 */
final class Comparators {

    private Comparators() {
    }

    /**
     * Comparator max/min (real collection max.pure: fold with STRICT
     * {@code >} — the FIRST max wins ties). The comparator must be a
     * KEY DIFFERENCE ({@code {x,y| f($x) - f($y)}}); the winner is the
     * element with the extreme key, earliest index on ties:
     * {@code (SELECT x FROM (UNNEST(l) x, UNNEST(range) i) ORDER BY key
     * DESC/ASC, i LIMIT 1)}.
     */
    static SqlExpr select(SqlExpr list, SqlExpr.Lambda cmp, boolean maxIn) {
        boolean max = maxIn;
        if (!(cmp.body() instanceof SqlExpr.Call mc) || mc.fn() != SqlFn.MINUS
                || mc.args().size() != 2) {
            throw new com.legend.error.NotImplementedException(
                    "comparator max/min beyond key-difference comparators"
                    + " ({x,y | f($x) - f($y)}) is not modeled");
        }
        String px = cmp.params().get(0);
        String py = cmp.params().get(1);
        SqlExpr keyOfX = mc.args().get(0);
        // the two sides must be the SAME key over the two params —
        // {x,y | f($x) - f($y)} ascending, {x,y | f($y) - f($x)} REVERSED
        SqlExpr rightAsX = Scalars.substituteRef(mc.args().get(1), py, SqlExpr.Column.derived(null, px));
        if (!keyOfX.equals(rightAsX)) {
            SqlExpr leftAsY = Scalars.substituteRef(mc.args().get(0), py, SqlExpr.Column.derived(null, px));
            SqlExpr rightSide = mc.args().get(1);
            if (leftAsY.equals(rightSide)
                    || Scalars.substituteRef(rightSide, px, SqlExpr.Column.derived(null, py)).equals(
                            Scalars.substituteRef(mc.args().get(0),
                                    px, SqlExpr.Column.derived(null, py)))) {
                // reversed comparator: max-by-it is MIN by the key
                keyOfX = Scalars.substituteRef(mc.args().get(1), px, SqlExpr.Column.derived(null, px));
                keyOfX = Scalars.substituteRef(keyOfX, py, SqlExpr.Column.derived(null, px));
                max = !max;
            } else {
                throw new IllegalStateException("comparator max/min: the two comparator"
                        + " sides must apply the SAME key to each parameter");
            }
        }
        // the element reference stamps as the list's element (§4bZ-U
        // leg 2 — the binding-door sweep: this site holds the list)
        SqlExpr.Column cx = list.type()
                instanceof com.legend.sql.TypeFact.Typed lt
                && lt.type() instanceof com.legend.sql.SqlType.Array at
                // §E3: element presence not provable — may-be-null
                ? SqlExpr.Column.of("_cx", "x", at.element(), true,
                        com.legend.sql.OutputCol.Origin.DERIVED)
                : SqlExpr.Column.derived("_cx", "x");
        SqlExpr keyOverElem = Scalars.substituteRef(keyOfX, px, cx);
        var inner = new SqlSelect(List.of(
                new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.UNNEST, list), "x", null),
                new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.UNNEST, SqlExpr.Call.of(SqlFn.RANGE_FN,
                                new SqlExpr.IntLit(1),
                                SqlExpr.Call.of(SqlFn.PLUS,
                                        SqlExpr.Call.of(SqlFn.LIST_LENGTH, list),
                                        new SqlExpr.IntLit(1)))), "i",
                        null)),
                false, new com.legend.sql.SqlSource.Dual(), null,
                            List.of(), null, null, List.of(), null, null, List.of());
        var src = new SqlSource.Subselect(inner, "_cx", null);
        var outer = new SqlSelect(List.of(
                new SqlSelect.Projection(cx, "w", null)),
                false, src, null, List.of(), null, null,
                List.of(new SqlSelect.SortKey(keyOverElem, !max,
                                SqlSelect.SortKey.NullOrder.NULLS_LAST, null),
                        SqlSelect.SortKey.asc(SqlExpr.Column.derived("_cx", "i"))),
                1L, null, List.of());
        return new SqlExpr.ScalarSubquery(outer);
    }

    /**
     * The direction of a bare-compare comparator: {@code {x,y|$x->compare($y)}}
     * ascending, {@code {x,y|$y->compare($x)}} descending; anything richer
     * has no relational sort shape (null).
     */
    static @com.legend.base.Nullable Boolean direction(TypedSpec spec) {
        if (!(spec instanceof TypedLambda cmp)
                || cmp.parameters().size() != 2 || cmp.body().size() != 1
                || !(cmp.body().get(0) instanceof TypedNativeCall cc)
                || !cc.callee().qualifiedName().equals("meta::pure::functions::lang::compare")
                || cc.args().size() != 2
                || !(cc.args().get(0) instanceof TypedVariable a)
                || !(cc.args().get(1) instanceof TypedVariable b)) {
            return null;
        }
        String p0 = cmp.parameters().get(0);
        String p1 = cmp.parameters().get(1);
        if (a.name().equals(p0) && b.name().equals(p1)) {
            return Boolean.TRUE;
        }
        if (a.name().equals(p1) && b.name().equals(p0)) {
            return Boolean.FALSE;
        }
        return null;
    }
}
