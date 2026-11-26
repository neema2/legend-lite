// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * LEGEND-PURE'S PRINT FORMS AT THE EDGE (phase D's D1.2, 2026-11-15): each product text node written as legend-pure's
 * form of it, which each product dialect spells for the judges already -- a Float's ({@link SqlFn#FLOAT_TO_TEXT}, lite's
 * six decimals) as legend-engine writes it in a query ({@link #floatOf}: the database's cast, a literal legend-pure's
 * print; Phase 5, 2026-11-16, until then {@link SqlFn#FLOAT_SHORTEST_TEXT}), a DateTime's ({@link SqlFn#DATETIME_TO_TEXT}, ISO with {@code Z}) as
 * legend-pure's print form ({@link PureDateTimePrint}; a literal's written text kept), a joined element's
 * ({@link SqlFn#CELL_TEXT}, D1.3: the product skips an empty one) as its text or {@code TDSNull}, and the texts only
 * legend-pure writes (D1.3: the product refuses them by name), {@code format}'s padded {@code %0<w>d}
 * (at lowering since B8 (i), 2026-11-17: {@code EdgePureOnlyTexts}) and an instance's {@code @_} id ({@link SqlFn#INSTANCE_TEXT}), as it writes them. The
 * edge's first pass, so every
 * later pass sees the tree the lowering wrote until D1.2: the edge's SQL is byte-identical to before.
 */
final class PurePrintForms extends com.legend.sql.SqlRewriter {

    private final FloatLiteral floatLiteral;

    private PurePrintForms(FloatLiteral floatLiteral) {
        this.floatLiteral = floatLiteral;
    }

    /** The edge's first pass, as a list a dialect returns, under the default Float literal processor. */
    static java.util.List<com.legend.sql.SqlRewriter> passes() {
        return passes(FloatLiteral.PRINT_BARE);
    }

    /** The edge's first pass, as a list a dialect returns, a Float literal under {@code toString} written by
     *  {@code floatLiteral}. */
    static java.util.List<com.legend.sql.SqlRewriter> passes(FloatLiteral floatLiteral) {
        return java.util.List.of(new PurePrintForms(floatLiteral), new TdsNullCells());
    }

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

    /** How a Float LITERAL reaches the database in legend-engine's SQL: its literal processor for a Float, per
     *  database (legend-engine 4.145.0, read). Lite writes a Float literal as a double (the edge too, since 2026-11-25:
     *  register S20), except at an extreme magnitude
     *  ({@link com.legend.sql.SqlTyping#floatDecimal}); under a {@code toString} the edge writes it as upstream does, so
     *  the database's cast prints what legend-engine's prints (Phase 5, row 8 of the edge program, 2026-11-16). */
    enum FloatLiteral {
        /** The default processor ({@code extensionDefaults.pure}'s {@code getDefaultLiteralProcessors}: format
         *  {@code %s}, legend-pure's {@code toString}), which DuckDB, Postgres, Snowflake, Databricks, BigQuery, Trino and
         *  ClickHouse keep: legend-pure's print, bare ({@link com.legend.result.CellText#carriedFloat}), an exact numeric
         *  the database prints as written ({@code 0.000000013421}, where a DOUBLE prints {@code 1.3421e-08}). The
         *  closest form where legend-engine has no extension (SQLite, MariaDB, MySQL, MongoDB). */
        PRINT_BARE,
        /** H2's processor ({@code h2Extension2_1_214.pure}'s {@code getLiteralProcessorsForH2}: format
         *  {@code CAST(%s AS FLOAT)}): legend-pure's print cast to H2's FLOAT, a double, which H2's cast prints
         *  {@code 1.3421E-8} (legend-engine's H2 manifest expects that failure of
         *  {@code testFloatToStringWithNegativeExponent}). */
        PRINT_AS_FLOAT;

        SqlExpr of(SqlExpr.FloatLit f) {
            if (!Double.isFinite(f.value())) {
                return f;
            }
            return switch (this) {
                case PRINT_BARE -> print(f);
                case PRINT_AS_FLOAT -> new SqlExpr.Cast(print(f), com.legend.sql.SqlType.Scalar.DOUBLE,
                        SqlExpr.Reason.DECLARED);
            };
        }

        private static SqlExpr print(SqlExpr.FloatLit f) {
            return new SqlExpr.DecimalLit(new java.math.BigDecimal(com.legend.result.CellText.carriedFloat(f.value())));
        }
    }

    /** A Float's {@code toString} in a query as legend-engine writes it, its {@code toString} dynaFunction: the
     *  database's cast ({@code cast(%s as varchar)}, legend-engine 4.145.0's {@code duckdbExtension.pure} and
     *  {@code h2Extension2_1_214.pure}), a literal operand written by {@code literal}. The pass's rule, and a node no
     *  pass reached. */
    static SqlExpr floatOf(SqlExpr x, FloatLiteral literal) {
        SqlExpr operand = x instanceof SqlExpr.FloatLit f ? literal.of(f) : x;
        return new SqlExpr.Cast(operand, com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER);
    }

    /** {@link #floatOf(SqlExpr, FloatLiteral)} under the default literal processor. */
    static SqlExpr floatOf(SqlExpr x) {
        return floatOf(x, FloatLiteral.PRINT_BARE);
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
            return floatOf(expr(c.args().get(0)), floatLiteral);
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.DATETIME_TO_TEXT) {
            // legend-pure's print form, composed here (census B12, 2026-11-17: the DATETIME_PRINT node gone)
            return PureDateTimePrint.of(expr(c.args().get(0)));
        }
        return super.expr(e);
    }
}
