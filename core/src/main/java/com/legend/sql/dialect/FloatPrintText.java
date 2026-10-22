// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/**
 * {@link SqlFn#FLOAT_PRINT} and {@link SqlFn#FLOAT_PRINT_ENGINE} spelled through a database's text of a double
 * ({@code CAST(v AS VARCHAR)}), the portable decompositions, moved unchanged from the lowering's
 * {@code LiteralSpelling.floatPrint} and {@code floatPrintJava} (2026-10-22). Each reads the text many times: Pure's
 * print form some nine times, legend-engine's text over a thousand, through the pieces it pastes. That is cheap where
 * a double's text is a CAST. A dialect whose double text is itself a long expression spells the calls its own way, the
 * value read once: Snowflake from its digits, BigQuery over its text bound once. Snowflake's PCT statement for
 * {@code 1.5 + 2.25} was 48 KB and its served one 6 MB on this path (measured 2026-10-22).
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
            if (e instanceof SqlExpr.Call c && (c.fn() == SqlFn.FLOAT_PRINT || c.fn() == SqlFn.FLOAT_PRINT_ENGINE)) {
                SqlExpr x = expr(c.args().get(0));
                // a double's text is the dialect's long expression; any other value's (a literal, a decimal slot) is
                // the database's own cast, read in place as before
                if (!(bind && isDouble(x))) {
                    return of(c.fn(), x);
                }
                // an equal call is the same node, its aliases too: a cell's text read again (a CSV cell's quoting
                // reads it five times) is then one subtree a common-subexpression binding writes once, as the canon's
                return spelledCalls.computeIfAbsent(c, k -> bound(k.fn(), x));
            }
            return super.expr(e);
        }

        private SqlExpr bound(SqlFn fn, SqlExpr x) {
            String inner = "__fp" + fresh[0]++;
            String outer = "__fp" + fresh[0]++;
            com.legend.sql.SqlSelect value = select(List.of(projection(x, "v", SqlType.Scalar.DOUBLE)),
                    new com.legend.sql.SqlSource.Dual());
            SqlExpr v = column(inner, "v", SqlType.Scalar.DOUBLE);
            com.legend.sql.SqlSelect texted = select(List.of(projection(v, "v", SqlType.Scalar.DOUBLE),
                    projection(new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR), "t", SqlType.Scalar.VARCHAR)),
                    new com.legend.sql.SqlSource.Subselect(value, inner, null));
            SqlExpr spelled = ofText(fn, column(outer, "v", SqlType.Scalar.DOUBLE),
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

    /** {@code fn}'s spelling of the double {@code x}, as SQL over its text. */
    static SqlExpr of(SqlFn fn, SqlExpr x) {
        return ofText(fn, x, new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR));
    }

    /** {@code fn}'s spelling of the double {@code x} whose text is {@code s}: {@code s} read where it is written, the
     *  value where it is. A dialect that binds the two once reads them as columns. */
    static SqlExpr ofText(SqlFn fn, SqlExpr x, SqlExpr s) {
        if (fn == SqlFn.FLOAT_PRINT) {
            return print(x, s);
        }
        if (fn == SqlFn.FLOAT_PRINT_ENGINE) {
            return engine(x, s);
        }
        throw new IllegalArgumentException(fn + " is not a Float's print form");
    }

    /**
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

    /**
     * legend-engine's text of a Float on {@code pure/v1} execute: Java's {@code Double.toString} layout over the
     * database's own shortest digits (the engine's serializer prints the double its driver read). Java prints plain
     * digits with at least one fraction digit when the magnitude is in [0.001, 10000000) ({@code 3.0}, {@code 0.001},
     * {@code 1234567.0}) and computerized scientific notation otherwise ({@code 1.0E7}, {@code 1.5E15},
     * {@code 2.5E-7}). The database spells it: its own text of the value (shortest round-trip digits in its own
     * layout: DuckDB's {@code 1.5e+15}, H2's {@code 1.5E15}, a decimal slot's {@code 2.50}) is taken apart into
     * its sign, its significant digits and the exponent of the first one, and laid out again. A text that is not a
     * number's is spelled as Java spells it ({@link #nonFinite}: {@code NaN}, {@code Infinity}, {@code -Infinity}).
     */
    private static SqlExpr engine(SqlExpr x, SqlExpr t) {
        SqlExpr body = SqlExpr.Call.of(SqlFn.LTRIM, t, text("-"));
        SqlExpr sign = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.LESS, x, new SqlExpr.IntLit(0)), text("-"))), text(""));
        SqlExpr epos = SqlExpr.Call.of(SqlFn.STRPOS, SqlExpr.Call.of(SqlFn.LOWER, body), text("e"));
        SqlExpr hasExp = SqlExpr.Call.of(SqlFn.GREATER, epos, new SqlExpr.IntLit(0));
        SqlExpr mant = new SqlExpr.Case(List.of(new SqlExpr.Case.When(hasExp,
                SqlExpr.Call.of(SqlFn.SUBSTRING, body, new SqlExpr.IntLit(1),
                        SqlExpr.Call.of(SqlFn.MINUS, epos, new SqlExpr.IntLit(1))))), body);
        SqlExpr exp = new SqlExpr.Case(List.of(new SqlExpr.Case.When(hasExp,
                new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.LTRIM, SqlExpr.Call.of(SqlFn.SUBSTRING, body,
                        SqlExpr.Call.of(SqlFn.PLUS, epos, new SqlExpr.IntLit(1)),
                        SqlExpr.Call.of(SqlFn.LENGTH, body)), text("+")), SqlType.Scalar.INTEGER))),
                new SqlExpr.IntLit(0));
        SqlExpr dot = SqlExpr.Call.of(SqlFn.STRPOS, mant, text("."));
        SqlExpr hasDot = SqlExpr.Call.of(SqlFn.GREATER, dot, new SqlExpr.IntLit(0));
        SqlExpr intPart = new SqlExpr.Case(List.of(new SqlExpr.Case.When(hasDot,
                SqlExpr.Call.of(SqlFn.SUBSTRING, mant, new SqlExpr.IntLit(1),
                        SqlExpr.Call.of(SqlFn.MINUS, dot, new SqlExpr.IntLit(1))))), mant);
        SqlExpr frac = new SqlExpr.Case(List.of(new SqlExpr.Case.When(hasDot,
                SqlExpr.Call.of(SqlFn.SUBSTRING, mant, SqlExpr.Call.of(SqlFn.PLUS, dot, new SqlExpr.IntLit(1)),
                        SqlExpr.Call.of(SqlFn.LENGTH, mant)))), text(""));
        SqlExpr digits = SqlExpr.Call.of(SqlFn.CONCAT, intPart, frac);
        SqlExpr noLead = SqlExpr.Call.of(SqlFn.LTRIM, digits, text("0"));
        SqlExpr lead = SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, digits),
                SqlExpr.Call.of(SqlFn.LENGTH, noLead));
        SqlExpr sig = SqlExpr.Call.of(SqlFn.RTRIM, noLead, text("0"));
        // the decimal exponent of the first significant digit
        SqlExpr e10 = SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.PLUS, exp,
                SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, intPart), new SqlExpr.IntLit(1))), lead);
        SqlExpr first = SqlExpr.Call.of(SqlFn.SUBSTRING, sig, new SqlExpr.IntLit(1), new SqlExpr.IntLit(1));
        SqlExpr rest = orZero(SqlExpr.Call.of(SqlFn.SUBSTRING, sig, new SqlExpr.IntLit(2),
                SqlExpr.Call.of(SqlFn.LENGTH, sig)));
        SqlExpr scientific = join(sign, first, text("."), rest, text("E"),
                new SqlExpr.Cast(e10, SqlType.Scalar.VARCHAR));
        SqlExpr intDigits = SqlExpr.Call.of(SqlFn.PLUS, e10, new SqlExpr.IntLit(1));
        SqlExpr plainWhole = join(sign,
                SqlExpr.Call.of(SqlFn.SUBSTRING, SqlExpr.Call.of(SqlFn.RPAD, sig, intDigits, text("0")),
                        new SqlExpr.IntLit(1), intDigits),
                text("."),
                orZero(SqlExpr.Call.of(SqlFn.SUBSTRING, sig, SqlExpr.Call.of(SqlFn.PLUS, e10, new SqlExpr.IntLit(2)),
                        SqlExpr.Call.of(SqlFn.LENGTH, sig))));
        SqlExpr plainFraction = join(sign, text("0."),
                SqlExpr.Call.of(SqlFn.RPAD, text(""), SqlExpr.Call.of(SqlFn.MINUS,
                        SqlExpr.Call.of(SqlFn.NEGATE, e10), new SqlExpr.IntLit(1)), text("0")),
                sig);
        List<SqlExpr.Case.When> whens = new java.util.ArrayList<>(nonFinite(t));
        whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, x, new SqlExpr.IntLit(0)), text("0.0")));
        whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND,
                        SqlExpr.Call.of(SqlFn.GREATER_EQUAL, e10, new SqlExpr.IntLit(-3)),
                        SqlExpr.Call.of(SqlFn.LESS, e10, new SqlExpr.IntLit(7))),
                        new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                                SqlExpr.Call.of(SqlFn.GREATER_EQUAL, e10, new SqlExpr.IntLit(0)), plainWhole)),
                                plainFraction)));
        return new SqlExpr.Case(whens, scientific);
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

    /** {@code e}, or {@code 0} when it is empty (Java's layout always writes a fraction digit). */
    private static SqlExpr orZero(SqlExpr e) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, e, text("")), text("0"))), e);
    }

    /** One n-ary CONCAT (every dialect renders its arguments flat): a tree nests the plan's JSON, a list does not. */
    private static SqlExpr join(SqlExpr... parts) {
        return parts.length == 1 ? parts[0] : SqlExpr.Call.of(SqlFn.CONCAT, parts);
    }
}
