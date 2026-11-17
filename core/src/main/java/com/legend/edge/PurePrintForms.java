// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * LEGEND-PURE'S PRINT FORMS AT THE EDGE (phase D's D1.2, 2026-11-15): each product text node written as legend-pure's
 * form of it, which each product dialect spells for the judges already -- a Float's ({@link SqlFn#FLOAT_TO_TEXT}, lite's
 * six decimals) as legend-engine writes it in a query ({@link #floatOf}: the database's cast, a literal legend-pure's
 * print; Phase 5, 2026-11-16, until then {@link SqlFn#FLOAT_PRINT}), a DateTime's ({@link SqlFn#DATETIME_TO_TEXT}, ISO with {@code Z}) as
 * {@link SqlFn#DATETIME_PRINT} (a literal's written text kept, its second operand), a joined element's
 * ({@link SqlFn#CELL_TEXT}, D1.3: the product skips an empty one) as its text or {@code TDSNull}, and the texts only
 * legend-pure writes (D1.3: the product refuses them by name), {@code format}'s padded {@code %0<w>d}
 * ({@link SqlFn#FORMAT_PADDED}) and an instance's {@code @_} id ({@link SqlFn#INSTANCE_TEXT}), as it writes them. The
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
     *  database (legend-engine 4.145.0, read). Lite writes a Float literal bare already, except at an extreme magnitude
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
        PRINT_AS_FLOAT,
        /** A dialect that spells a Float literal as legend-engine's processor already ({@code EngineStyleH2}'s
         *  {@code cast(x as float)}): the literal as it is. */
        AS_SPELLED;

        SqlExpr of(SqlExpr.FloatLit f) {
            if (!Double.isFinite(f.value())) {
                return f;
            }
            return switch (this) {
                case PRINT_BARE -> print(f);
                case PRINT_AS_FLOAT -> new SqlExpr.Cast(print(f), com.legend.sql.SqlType.Scalar.DOUBLE,
                        SqlExpr.Reason.DECLARED);
                case AS_SPELLED -> f;
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

    /**
     * A DateTime cell's text in legend-pure's TDS text (a relation's {@code toString}; {@link SqlFn#DATETIME_TDS_TEXT}):
     * legend-pure's text of a DateTime, with AT LEAST THREE fraction digits -- {@code 2024-01-02T10:30:00.000+0000}
     * for a whole second, {@code .123} for a millisecond, {@code .123456789} for a nanosecond, finer trailing zeros
     * trimmed. The reference is legend-engine's own test (core_functions_standard/date/extract/dayOfWeek.pure,
     * {@code testDayOfWeek_Relation}): a TDS literal's {@code 1985-01-12T06:30:00Z} prints
     * {@code 1985-01-12T06:30:00.000+0000} in the relation's text, where the value's own text (interpreted Pure's
     * {@code toString} of {@code %1985-01-12T06:30:00}) has no fraction. Measured 2026-10-18 (the plan path program's
     * step 6, channel B's relation and standard suites: 35 goldens spell the three digits). Moved here from the
     * lowering (LiteralSpelling.dateTimeTextTds) unchanged, F's separation census line B11, 2026-11-17.
     */
    private static SqlExpr dateTimeTextTds(SqlExpr x) {
        SqlExpr nine = SqlExpr.Call.of(SqlFn.STRFTIME, x,
                new SqlExpr.FormatLit(java.util.List.of(com.legend.sql.DateFmt.Part.SUBSEC_NANO)));
        SqlExpr three = SqlExpr.Call.of(SqlFn.SUBSTRING, nine, new SqlExpr.IntLit(1), new SqlExpr.IntLit(3));
        SqlExpr finer = SqlExpr.Call.of(SqlFn.RTRIM,
                SqlExpr.Call.of(SqlFn.SUBSTRING, nine, new SqlExpr.IntLit(4), new SqlExpr.IntLit(6)),
                new SqlExpr.StringLit("0"));
        return SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT,
                        SqlExpr.Call.of(SqlFn.STRFTIME, x,
                                new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_LOCAL)),
                        SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("."), three),
                                finer)),
                new SqlExpr.StringLit("+0000"));
    }

    /** A date carried as its own text ({@link SqlFn#DATETIME_TEXT_OF_TEXT}: a value root spelled at the precision it
     *  has: a year, a month, a day, an hour, a minute, a second or finer; with or without Pure's {@code %}; a literal's
     *  print form with its {@code +0000} already): one with seconds and no zone takes Pure's {@code +0000}, as Pure
     *  prints a DateTime; every other precision is its text, Pure's DateTime literal taking a zone only after seconds
     *  (the MongoDB lane, 2026-10-18: {@code %1973-11-13T23} and {@code %2025-02-10T20:10:20+0000} are read as
     *  written). Register S121 (pending: legend-pure writes +0000 from the minute). Moved here from the lowering
     *  (LiteralSpelling.dateTimeTextOfText) unchanged, B11, 2026-11-17. */
    private static SqlExpr dateTimeTextOfText(SqlExpr text) {
        SqlExpr colons = SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, text),
                SqlExpr.Call.of(SqlFn.LENGTH, SqlExpr.Call.of(SqlFn.REPLACE, text, new SqlExpr.StringLit(":"),
                        new SqlExpr.StringLit(""))));
        SqlExpr secondsNoZone = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL, colons, new SqlExpr.IntLit(2)),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.ENDS_WITH, text, new SqlExpr.StringLit("+0000"))));
        return new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(secondsNoZone,
                SqlExpr.Call.of(SqlFn.CONCAT, text, new SqlExpr.StringLit("+0000")))), text, SqlExpr.Reason.PRINT);
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
            java.util.List<SqlExpr> args = new java.util.ArrayList<>(c.args().size());
            for (SqlExpr a : c.args()) {
                args.add(expr(a));
            }
            return new SqlExpr.Call(SqlFn.DATETIME_PRINT, args);
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.DATETIME_TDS_TEXT) {
            return dateTimeTextTds(c.args().get(0));
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.DATETIME_TEXT_OF_TEXT) {
            return dateTimeTextOfText(c.args().get(0));
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
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.REPRESENTATION_TEXT) {
            // legend-pure's representation, as the lowering writes it (Phase 3, §10 item 8: the product refuses it)
            return expr(c.args().get(0));
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.INSTANCE_TEXT) {
            return SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("@_"), expr(c.args().get(0)));
        }
        return super.expr(e);
    }
}
