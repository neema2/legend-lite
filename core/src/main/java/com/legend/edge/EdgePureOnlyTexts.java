// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * THE TEXTS ONLY LEGEND-PURE WRITES, COMPOSED AT LOWERING (the separation census's B8 (i), 2026-11-17): the edge's
 * answer to the plan style's {@link com.legend.lowering.PureOnlyTexts} facet, each text built from the product's own
 * nodes -- what the edge's first spelling pass (PurePrintForms) wrote for the lowering's marker node until then, so
 * the SQL is byte-identical.
 */
final class EdgePureOnlyTexts implements com.legend.lowering.PureOnlyTexts {

    static final EdgePureOnlyTexts INSTANCE = new EdgePureOnlyTexts();

    private EdgePureOnlyTexts() {
    }

    /** {@code format}'s {@code %0<w>d} as legend-pure writes it ({@code -3} at width 5 is {@code -00003}): the digits
     *  padded, then signed -- printf's width would count the sign (moved here from the lowering, Scalars, 2026-11-15:
     *  phase D's D1.3; from the edge's spelling pass to its lowering facet, B8 (i), 2026-11-17). */
    @Override
    public SqlExpr formatPadded(SqlExpr x, SqlExpr.IntLit width) {
        SqlExpr padded = SqlExpr.Call.of(SqlFn.LPAD,
                new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.ABS, x), com.legend.sql.SqlType.Scalar.VARCHAR,
                        SqlExpr.Reason.PRINT),
                width, new SqlExpr.StringLit("0"));
        return new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.LESS, x, new SqlExpr.IntLit(0)),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("-"), padded))),
                padded, SqlExpr.Reason.PRINT);
    }

    /** A user's {@code toRepresentation}: legend-pure's representation as the lowering writes it, as it is (the
     *  edge's pass unwrapped REPRESENTATION_TEXT to it until B8 (i), 2026-11-17). */
    @Override
    public SqlExpr representation(SqlExpr text) {
        return text;
    }
}
