// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * LEGEND-PURE'S PRINT FORMS AT THE EDGE (phase D's D1.2, 2026-11-15): each product text node written as legend-pure's
 * form of it, which each product dialect spells for the judges already -- a Float's ({@link SqlFn#FLOAT_TO_TEXT}, lite's
 * six decimals) as {@link SqlFn#FLOAT_PRINT}, a DateTime's ({@link SqlFn#DATETIME_TO_TEXT}, ISO with {@code Z}) as
 * {@link SqlFn#DATETIME_PRINT} (a literal's written text kept, its second operand). The edge's first pass, so every
 * later pass sees the tree the lowering wrote until D1.2: the edge's SQL is byte-identical to before.
 */
final class PurePrintForms extends com.legend.sql.SqlRewriter {

    private PurePrintForms() {
    }

    /** The edge's first pass, as a list a dialect returns. */
    static java.util.List<com.legend.sql.SqlRewriter> passes() {
        return java.util.List.of(new PurePrintForms());
    }

    /** {@code x}'s legend-pure print form as a Float: the call a pass writes, for a node no pass reached. */
    static SqlExpr floatOf(SqlExpr x) {
        return SqlExpr.Call.of(SqlFn.FLOAT_PRINT, x);
    }

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.FLOAT_TO_TEXT) {
            return floatOf(expr(c.args().get(0)));
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.DATETIME_TO_TEXT) {
            java.util.List<SqlExpr> args = new java.util.ArrayList<>(c.args().size());
            for (SqlExpr a : c.args()) {
                args.add(expr(a));
            }
            return new SqlExpr.Call(SqlFn.DATETIME_PRINT, args);
        }
        return super.expr(e);
    }
}
