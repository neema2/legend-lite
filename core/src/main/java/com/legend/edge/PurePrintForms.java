// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * LEGEND-PURE'S PRINT FORMS AT THE EDGE (phase D's D1.2, 2026-11-15): each product text node written as legend-pure's
 * form of it, which each product dialect spells for the judges already -- a Float's ({@link SqlFn#FLOAT_TO_TEXT}, lite's
 * six decimals) as {@link SqlFn#FLOAT_PRINT}, a DateTime's ({@link SqlFn#DATETIME_TO_TEXT}, ISO with {@code Z}) as
 * {@link SqlFn#DATETIME_PRINT} (a literal's written text kept, its second operand), a joined element's
 * ({@link SqlFn#CELL_TEXT}, D1.3: the product skips an empty one) as its text or {@code TDSNull}, and the texts only
 * legend-pure writes (D1.3: the product refuses them by name), {@code format}'s padded {@code %0<w>d}
 * ({@link SqlFn#FORMAT_PADDED}) and an instance's {@code @_} id ({@link SqlFn#INSTANCE_TEXT}), as it writes them. The
 * edge's first pass, so every
 * later pass sees the tree the lowering wrote until D1.2: the edge's SQL is byte-identical to before.
 */
final class PurePrintForms extends com.legend.sql.SqlRewriter {

    private PurePrintForms() {
    }

    /** The edge's first pass, as a list a dialect returns. */
    static java.util.List<com.legend.sql.SqlRewriter> passes() {
        return java.util.List.of(new PurePrintForms(), new TdsNullCells());
    }

    /** The edge's pass after {@link PurePrintForms}: a {@link SqlFn#CELL_TEXT} no join read (a TDS cell's
     *  {@code toString}), legend-engine's {@code TDSNull} where the cell is empty (D1.3, 2026-11-15: until then the
     *  compiler's desugar wrote {@code if(isEmpty(cell), 'TDSNull', toString(cell))}, which this answers alike). */
    private static final class TdsNullCells extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlExpr expr(SqlExpr e) {
            return isJoined(e) ? tdsNullOf(e) : e;
        }
    }

    /** {@code x}'s legend-pure print form as a Float: the call a pass writes, for a node no pass reached. */
    static SqlExpr floatOf(SqlExpr x) {
        return SqlExpr.Call.of(SqlFn.FLOAT_PRINT, x);
    }

    /** {@code format}'s {@code %0<w>d} as legend-pure writes it ({@code -3} at width 5 is {@code -00003}): the digits
     *  padded, then signed -- printf's width would count the sign (moved here from the lowering, Scalars, 2026-11-15:
     *  phase D's D1.3). */
    private static SqlExpr signedZeroPad(SqlExpr x, SqlExpr width) {
        SqlExpr padded = SqlExpr.Call.of(SqlFn.LPAD,
                new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.ABS, x), com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT),
                width, new SqlExpr.StringLit("0"));
        return new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.LESS, x, new SqlExpr.IntLit(0)),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("-"), padded))),
                padded, SqlExpr.Reason.PRINT);
    }

    /** A join's element texts as legend-engine writes them (phase D's D1.3: the product skips an empty one), at the
     *  join -- the walk is bottom-up, so the join reads its elements' {@link SqlFn#CELL_TEXT}s: a list's elements,
     *  each {@code COALESCE(x, 'TDSNull')}; a row's cells ({@code COALESCE(STRING_AGG(<the cells>, sep), '')}),
     *  legend-engine's interleave, the cells and the separator concatenated -- the lowering's forms until 2026-11-15
     *  (an empty separator, the one-argument {@code makeString}'s, is no part). Null where {@code e} is neither. */
    private static @com.legend.base.Nullable SqlExpr joined(SqlExpr e) {
        if (e instanceof SqlExpr.ReduceCollection rc && rc.reducer() == com.legend.sql.SqlAgg.Fn.STRING_AGG
                && rc.collection() instanceof SqlExpr.Call t && t.fn() == SqlFn.LIST_TRANSFORM && t.args().size() == 2
                && t.args().get(1) instanceof SqlExpr.Lambda l && isJoined(l.body())) {
            return new SqlExpr.ReduceCollection(rc.reducer(), SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, t.args().get(0),
                    new SqlExpr.Lambda(l.params(), tdsNullOf(l.body()))), rc.extras());
        }
        if (!(e instanceof SqlExpr.Call c && c.fn() == SqlFn.COALESCE && c.args().size() == 2
                && c.args().get(0) instanceof SqlExpr.ReduceCollection rc
                && rc.reducer() == com.legend.sql.SqlAgg.Fn.STRING_AGG && rc.extras().size() == 1
                && rc.collection() instanceof SqlExpr.ArrayLit cells && !cells.elements().isEmpty()
                && cells.elements().stream().allMatch(PurePrintForms::isJoined))) {
            return null;
        }
        SqlExpr sep = rc.extras().get(0) instanceof SqlExpr.StringLit s && s.value().isEmpty() ? null
                : rc.extras().get(0);
        java.util.List<SqlExpr> parts = new java.util.ArrayList<>();
        for (SqlExpr cell : cells.elements()) {
            if (!parts.isEmpty() && sep != null) {
                parts.add(sep);
            }
            parts.add(tdsNullOf(cell));
        }
        return parts.size() == 1 ? parts.get(0) : new SqlExpr.Call(SqlFn.CONCAT, parts);
    }

    private static boolean isJoined(SqlExpr e) {
        return e instanceof SqlExpr.Call c && c.fn() == SqlFn.CELL_TEXT;
    }

    /** A {@link SqlFn#CELL_TEXT}'s operand, legend-engine's {@code TDSNull} where it is empty. */
    private static SqlExpr tdsNullOf(SqlExpr joined) {
        return SqlExpr.Call.of(SqlFn.COALESCE, ((SqlExpr.Call) joined).args().get(0), new SqlExpr.StringLit(Presenter.TDS_NULL));
    }

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (joined(e) instanceof SqlExpr j) {
            return j;
        }
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
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.FORMAT_PADDED) {
            return signedZeroPad(expr(c.args().get(0)), c.args().get(1));
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.TRACE_TEXT) {
            // legend-engine's trace comment around the execution's id (its executor's, RelationalExecutor.java:332;
            // D1.4, 2026-11-15: the runner bound it until then)
            return new SqlExpr.Call(SqlFn.CONCAT, java.util.List.of(new SqlExpr.StringLit("-- \"executionTraceID\" : \""),
                    c.args().get(0), new SqlExpr.StringLit("\"")));
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.TEXT_FORMS) {
            return c.args().get(1);
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.RELATION_TEXT) {
            return c.args().get(0);
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.INSTANCE_TEXT) {
            return SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("@_"), expr(c.args().get(0)));
        }
        return super.expr(e);
    }
}
