// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * LEGEND-PURE'S TEXT OF A FLOAT AT THE EDGE (phase D's D1.2, 2026-11-15): the product's {@link SqlFn#FLOAT_TO_TEXT} --
 * lite's six decimals, trimmed (the user's ruling of 2026-11-10) -- written as {@link SqlFn#FLOAT_PRINT}, legend-pure's
 * print form, which each product dialect spells for the judges' {@code %r} already. The edge's first pass, so every
 * later pass (the print form's own expansion first) sees the tree the lowering wrote until D1.2: the edge's SQL is
 * byte-identical to before.
 */
final class FloatToPrint extends com.legend.sql.SqlRewriter {

    private FloatToPrint() {
    }

    /** The edge's first pass, as a list a dialect returns. */
    static java.util.List<com.legend.sql.SqlRewriter> passes() {
        return java.util.List.of(new FloatToPrint());
    }

    /** {@code x}'s legend-pure print form: the call a pass writes, for a node no pass reached. */
    static SqlExpr of(SqlExpr x) {
        return SqlExpr.Call.of(SqlFn.FLOAT_PRINT, x);
    }

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.FLOAT_TO_TEXT) {
            return of(expr(c.args().get(0)));
        }
        return super.expr(e);
    }
}
