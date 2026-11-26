// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * LEGEND-PURE'S PRINT FORMS AT THE EDGE (phase D's D1.2, 2026-11-15): a product text node written as legend-pure's form
 * of it -- a DateTime's ({@link SqlFn#DATETIME_TO_TEXT}, ISO with {@code Z}) as legend-pure's print form
 * ({@link PureDateTimePrint}; a literal's written text kept), and a joined element's ({@link SqlFn#CELL_TEXT}, D1.3: the
 * product skips an empty one) as its text or {@code TDSNull}. (A Float's text, legend-engine's in-query cast, was this
 * pass's until 2026-11-26: the note below. The texts only legend-pure writes are composed at lowering since B8 (i),
 * 2026-11-17: {@code EdgePureOnlyTexts}.) The edge's first pass, so every later pass sees the tree the lowering wrote.
 */
final class PurePrintForms extends com.legend.sql.SqlRewriter {

    private PurePrintForms() {
    }

    /** The edge's first pass, as a list a dialect returns. */
    static java.util.List<com.legend.sql.SqlRewriter> passes() {
        return java.util.List.of(new PurePrintForms(), new TdsNullCells());
    }

    // (The Float text: gone 2026-11-26, the edition program's step 8. Until then this pass wrote legend-engine's
    // in-query cast for every Float text of every compatibility plan, a Float literal by legend-engine's literal
    // processor of the database (FloatLiteral: bare, or on H2 cast to FLOAT), and each edge dialect overrode
    // floatToText for a node no pass reached. Now: inside a store query at legend-engine's door that cast is a row of
    // legend-engine's table (EngineTable; lowering.NamedRule FLOAT_TEXT_BY_CAST and FLOAT_TEXT_BY_CAST_OF_A_DOUBLE; the
    // user's ruling of 2026-11-26, "agreed on float/date text for engine"); at legend-pure's doors a Float's print at a
    // last step is Java's (PrintedRelations, PureValueText) and one used further is lite's own text, written by the
    // product's dialect (tenet 1 as the user amended it on 2026-11-25; the register row of this step).)

    // (F31's pass, the unguarded parses: a parse written as upstream's plain cast in every edge plan. Gone 2026-11-26,
    // the edition program's step 5: the plain cast is a row of legend-engine's table, EngineTable -- inside a store
    // query alone -- and PCT's legend-pure run and a host-side let keep the product's checked parse.)

    /** The edge's pass after {@link PurePrintForms}: a {@link SqlFn#CELL_TEXT} no join read (a TDS cell's
     *  {@code toString}), legend-engine's {@code TDSNull} where the cell is empty (D1.3, 2026-11-15: until then the
     *  compiler's desugar wrote {@code if(isEmpty(cell), 'TDSNull', toString(cell))}, which this answers alike). */
    private static final class TdsNullCells extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlExpr expr(SqlExpr e) {
            return isJoined(e) ? tdsNullOf(e) : e;
        }
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
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.DATETIME_TO_TEXT) {
            // legend-pure's print form, composed here (census B12, 2026-11-17: the DATETIME_PRINT node gone)
            return PureDateTimePrint.of(expr(c.args().get(0)));
        }
        return super.expr(e);
    }
}
