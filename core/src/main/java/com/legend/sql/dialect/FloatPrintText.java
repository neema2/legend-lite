// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/**
 * {@link SqlFn#FLOAT_PRINT} spelled through a database's text of a double ({@code CAST(v AS VARCHAR)}), the portable
 * decomposition, moved unchanged from the lowering's {@code LiteralSpelling.floatPrint} (2026-10-22). It reads the
 * text some nine times through the pieces it pastes. That is cheap where a double's text is a CAST. A dialect whose
 * double text is itself a long expression spells the call its own way, the value read once: Snowflake from its
 * digits, BigQuery over its text bound once. Snowflake's PCT statement for {@code 1.5 + 2.25} was 48 KB on this path
 * (measured 2026-10-22). (legend-engine's text, the {@code FLOAT_PRINT_ENGINE} call, went with the database-written
 * wires in the result boundary program's phase 3, 2026-10-27: the engine writer spells it in Java, result.CellText.)
 */
final class FloatPrintText {

    private FloatPrintText() {
    }

    /**
     * The FIRST pass of a dialect that does not spell the two calls itself (AnsiSqlRenderer.renderPasses): each call
     * becomes {@link #of}, so every later pass sees the tree the lowering wrote before the calls existed, and the SQL is
     * the same.
     */
    static final class Expansion extends com.legend.sql.SqlRewriter {

        private final boolean bind;
        private final int[] fresh = {0};
        private final java.util.Map<SqlExpr.Call, SqlExpr> spelledCalls = new java.util.HashMap<>();

        /** {@code bind}: the value and its text are columns of a one-row derived table, each written once, under a
         *  scalar subquery ({@code (SELECT <spelling over v and t> FROM (SELECT v, CAST(v AS VARCHAR) AS t FROM
         *  (SELECT <value> AS v) AS __fpN) AS __fpN)}, the shape the Float canon's binding takes); else the spelling
         *  reads them in place. */
        Expansion(boolean bind) {
            this.bind = bind;
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.FLOAT_PRINT) {
                SqlExpr x = expr(c.args().get(0));
                // a double's text is the dialect's long expression; any other value's (a literal, a decimal slot) is
                // the database's own cast, read in place as before
                if (!(bind && isDouble(x))) {
                    return of(x);
                }
                // an equal call is the same node, its aliases too: a cell's text read again (a CSV cell's quoting
                // reads it five times) is then one subtree a common-subexpression binding writes once, as the canon's
                return spelledCalls.computeIfAbsent(c, k -> bound(x));
            }
            return super.expr(e);
        }

        private SqlExpr bound(SqlExpr x) {
            String inner = "__fp" + fresh[0]++;
            String outer = "__fp" + fresh[0]++;
            com.legend.sql.SqlSelect value = select(List.of(projection(x, "v", SqlType.Scalar.DOUBLE)),
                    new com.legend.sql.SqlSource.Dual());
            SqlExpr v = column(inner, "v", SqlType.Scalar.DOUBLE);
            com.legend.sql.SqlSelect texted = select(List.of(projection(v, "v", SqlType.Scalar.DOUBLE),
                    projection(new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR), "t", SqlType.Scalar.VARCHAR)),
                    new com.legend.sql.SqlSource.Subselect(value, inner, null));
            SqlExpr spelled = print(column(outer, "v", SqlType.Scalar.DOUBLE),
                    column(outer, "t", SqlType.Scalar.VARCHAR));
            return new SqlExpr.ScalarSubquery(select(List.of(projection(spelled, "p", SqlType.Scalar.VARCHAR)),
                    new com.legend.sql.SqlSource.Subselect(texted, outer, null)));
        }

        private static boolean isDouble(SqlExpr x) {
            return x.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() == SqlType.Scalar.DOUBLE;
        }

        private static com.legend.sql.SqlSelect.Projection projection(SqlExpr e, String name, SqlType type) {
            return new com.legend.sql.SqlSelect.Projection(e, name, new com.legend.sql.OutputCol(name, type, true));
        }

        private static com.legend.sql.SqlSelect select(List<com.legend.sql.SqlSelect.Projection> projections,
                com.legend.sql.SqlSource from) {
            return new com.legend.sql.SqlSelect(projections, false, from, null, List.of(), null, null, List.of(), null,
                    null, List.of());
        }

        private static SqlExpr column(String alias, String name, SqlType type) {
            return SqlExpr.Column.of(alias, name, type, true, com.legend.sql.OutputCol.Origin.DERIVED);
        }
    }

    /** Pure's print form of the double {@code x}, as SQL over its text. */
    static SqlExpr of(SqlExpr x) {
        return print(x, new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR));
    }

    /**
     * Pure's print form of the double {@code x} whose text is {@code s}: {@code s} read where it is written, the value
     * where it is (a dialect that binds the two once reads them as columns; {@code ofText}, a pass-through to here whose
     * function parameter had gone, deleted 2026-10-28: the phase 3 audit's nit, the start page's S1.4 item 4).
     * Pure prints a Float via its MINIMAL decimal repr: DuckDB's shortest round-trip VARCHAR cast already matches
     * ('1.5', '2.0') EXCEPT where it chooses exponent notation -- those re-render plain through a DECIMAL(38,18) cast
     * with trailing zeros trimmed (and a bare trailing dot restored to '.0'). Magnitudes outside DECIMAL(38,18) keep the
     * exponent form.
     */
    private static SqlExpr print(SqlExpr x, SqlExpr s) {
        // FRACTION-FREE values render through HUGEINT — exact plain digits
        // for the whole [1e16, 1e38) band where the DECIMAL(38,18) cast
        // fabricates garbage (audit: 1e18 printed ...042.42...); every
        // double >= 2^53 is fraction-free, so all large magnitudes take
        // this branch.
        // the exponent marker in either case: DuckDB and Postgres write 2.5e-07, H2 1.5E15 (the output matrix,
        // docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §3 gap 2, 2026-10-18)
        SqlExpr hasExp = SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, SqlExpr.Call.of(SqlFn.LOWER, s), new SqlExpr.StringLit("e")),
                new SqlExpr.IntLit(0));
        SqlExpr fractionFree = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, x, SqlExpr.Call.of(SqlFn.FLOOR_RAW, x)),
                SqlExpr.Call.of(SqlFn.LESS,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(1e38)));
        // The DECIMAL path stays only where the scale-18 cast is exact for
        // short-decimal values: fractional magnitudes in [1e-17, 2^53)
        // (below 1e-17 the scale rounds — 1.5e-18 gained a digit; audit).
        SqlExpr inRange = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(1e-17)),
                SqlExpr.Call.of(SqlFn.LESS,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(9.007199254740992e15)));
        // each cast reads the value only where its branch is taken (NULL elsewhere, never read): a database that
        // computes every branch of a CASE -- ClickHouse, whose constant folding casts a constant whatever its guard --
        // otherwise failed the cast of a value the branch never takes (cosh(-100), 1.3E43, out of Decimal(38,18):
        // DECIMAL_OVERFLOW; step 9, 2026-10-19, the ClickHouse lane on the plan route)
        SqlExpr intPlain = SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.Cast(new SqlExpr.Cast(onlyWhere(fractionFree, x), SqlType.Scalar.HUGEINT),
                        SqlType.Scalar.VARCHAR),
                new SqlExpr.StringLit(".0"));
        SqlExpr plain = SqlExpr.Call.of(SqlFn.RTRIM,
                new SqlExpr.Cast(new SqlExpr.Cast(onlyWhere(inRange, x), new SqlType.Decimal(38, 18)),
                        SqlType.Scalar.VARCHAR),
                new SqlExpr.StringLit("0"));
        SqlExpr fixed = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.ENDS_WITH, plain, new SqlExpr.StringLit(".")),
                SqlExpr.Call.of(SqlFn.CONCAT, plain, new SqlExpr.StringLit("0")))),
                plain);
        // a whole value the database writes without a fraction (Postgres's double 3, an integer slot a Float
        // declares) takes Pure's .0: a Float's text always has a fraction (the output layer's step 5, 2026-10-18)
        SqlExpr bare = SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.NOT, hasExp),
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.STRPOS, s, new SqlExpr.StringLit(".")),
                        new SqlExpr.IntLit(0)));
        List<SqlExpr.Case.When> whens = new java.util.ArrayList<>(nonFinite(s));
        whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, hasExp, fractionFree), intPlain));
        whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, hasExp, inRange), fixed));
        whens.add(new SqlExpr.Case.When(bare, SqlExpr.Call.of(SqlFn.CONCAT, s, new SqlExpr.StringLit(".0"))));
        return new SqlExpr.Case(whens, s);
    }

    /** {@code x} where {@code guard} holds, else NULL. */
    private static SqlExpr onlyWhere(SqlExpr guard, SqlExpr x) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(guard, x)), null);
    }

    /** The text of a double that is no number, in Pure's spelling, which is Java's ({@code NaN}, {@code Infinity},
     *  {@code -Infinity}), from the database's own: DuckDB writes {@code nan}, {@code inf} and {@code -inf}, H2 and
     *  Postgres write Java's (the channel A shadow run of 2026-10-18 found {@code acos(2.0)} printed {@code nan.0};
     *  the plan path program's step 7). The spelled tokens are compared, nothing computed (docs/WORLD_MAP.md). */
    private static List<SqlExpr.Case.When> nonFinite(SqlExpr s) {
        SqlExpr lower = SqlExpr.Call.of(SqlFn.LOWER, s);
        return List.of(
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IN, lower, text("nan"), text("-nan")), text("NaN")),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IN, lower, text("inf"), text("+inf"), text("infinity")),
                        text("Infinity")),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IN, lower, text("-inf"), text("-infinity")),
                        text("-Infinity")));
    }

    private static SqlExpr text(String s) {
        return new SqlExpr.StringLit(s);
    }

}
