// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/**
 * {@link SqlFn#FLOAT_CANON} spelled through a database's text of a double ({@code CAST(v AS VARCHAR)}), the portable
 * decomposition (moved from the lowering's LiteralSpelling.floatCanon, 2026-10-11, unchanged): Pure's literal
 * spelling of a Float -- every zero {@code 0.0}, the exponent unfolded to fixed point, at least one fraction digit.
 *
 * <p>It reads the text many times (each test and each piece of the unfold), which is cheap where a double's text is a
 * CAST; a dialect whose double text is itself a long expression spells the call its own way, the value read once
 * (MariaDb).
 */
final class FloatCanonText {

    private FloatCanonText() {
    }

    /**
     * The FIRST pass of a dialect that does not spell {@link SqlFn#FLOAT_CANON} itself (AnsiSqlRenderer.renderPasses):
     * each call becomes {@link #of}, so every later pass -- the carrier strategies, the substring clamp, a common-
     * subexpression binding -- sees the tree the lowering wrote before the call existed, and the SQL is the same.
     */
    static final class Expansion extends com.legend.sql.SqlRewriter {

        private final boolean bindText;
        private final int[] fresh = {0};
        private final java.util.Map<SqlExpr, SqlExpr> spelledCalls = new java.util.HashMap<>();

        /** {@code bindText}: the double's text is a column of a one-row derived table, written once, under a scalar
         *  subquery ({@code (SELECT <spelling over t> FROM (SELECT CAST(v AS VARCHAR) AS t) AS __fcN)}, the shape a
         *  common-subexpression binding takes); else the spelling reads the text in place. */
        Expansion(boolean bindText) {
            this.bindText = bindText;
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.FLOAT_CANON) {
                if (!bindText) {
                    return of(c.args().get(0));
                }
                // an equal call is the same node, its alias too: a repeated canon is then one subtree a common-
                // subexpression binding writes once (step 9, 2026-10-19: a mixed list's float decoded in each branch
                // of format was 1.29 MB on SQLite, each copy under its own alias, so never equal)
                SqlExpr known = spelledCalls.get(c);
                if (known != null) {
                    return known;
                }
                String alias = "__fc" + fresh[0]++;
                SqlExpr text = new SqlExpr.Cast(c.args().get(0), SqlType.Scalar.VARCHAR);
                com.legend.sql.OutputCol t = new com.legend.sql.OutputCol("t", SqlType.Scalar.VARCHAR, true);
                SqlExpr ref = SqlExpr.Column.of(alias, "t", SqlType.Scalar.VARCHAR, true,
                        com.legend.sql.OutputCol.Origin.DERIVED);
                com.legend.sql.SqlSelect bind = new com.legend.sql.SqlSelect(
                        List.of(new com.legend.sql.SqlSelect.Projection(text, "t", t)), false,
                        new com.legend.sql.SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                        List.of());
                SqlExpr spelled = of(ref);
                com.legend.sql.SqlSelect use = new com.legend.sql.SqlSelect(
                        List.of(new com.legend.sql.SqlSelect.Projection(spelled, "v",
                                new com.legend.sql.OutputCol("v", SqlType.Scalar.VARCHAR, true))), false,
                        new com.legend.sql.SqlSource.Subselect(bind, alias, null), null, List.of(), null, null, List.of(),
                        null, null, List.of());
                SqlExpr spelledCall = new SqlExpr.ScalarSubquery(use);
                spelledCalls.put(c, spelledCall);
                return spelledCall;
            }
            return super.expr(e);
        }
    }

    /** Pure's literal spelling of the double {@code v}, as SQL over its text. */
    static SqlExpr of(SqlExpr v) {
        return ofText(new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR));
    }

    /** The exponent unfolded to fixed point where the text {@code base} carries one, else the text as it is: a
     *  database whose text of a number is scientific past some magnitude (MongoDB's {@code $toString} of a double, a
     *  Decimal128's own form) reads its number through this (the MongoDB line's homework 3, 2026-10-17). */
    static SqlExpr fixedPoint(SqlExpr base) {
        return new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.OR, has(base, "e"), has(base, "E")),
                        exponentUnfold(base))),
                base);
    }

    /** Pure's literal spelling of a double over {@code base}, the database's text of it. */
    static SqlExpr ofText(SqlExpr base) {
        SqlExpr unfolded = exponentUnfold(base);
        return new SqlExpr.Case(List.of(
                // ZEROS UNIFY (spec §3, witness parseFloat('-000.000')):
                // pure grants 0.0 == -0.0, so the canonical render of
                // every zero is '0.0'. Detected TEXTUALLY (F10 slice 1):
                // the old v = 0.0 compare forced SQL to cast the COLUMN
                // to DOUBLE, which errored the whole wrapped query on
                // print-form identity carriers ('7.345D') — the canon
                // must be TOTAL over any column it can meet. For genuine
                // DOUBLE columns the zero texts are exactly 0.0/-0.0,
                // so the regex is equivalence, not leniency.
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.REGEXP_FULL_MATCH, base,
                                new SqlExpr.StringLit("-?0+(\\.0+)?")),
                        new SqlExpr.StringLit("0.0")),
                // both wire spellings of the exponent: DuckDB's 1.3421e-08,
                // H2's 1.3421E-8 / 1.0E7 (a DOUBLE prints as Java does)
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.OR, has(base, "e"), has(base, "E")),
                        unfolded),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.NOT,
                        has(base, ".")),
                        SqlExpr.Call.of(SqlFn.CONCAT, base,
                                new SqlExpr.StringLit(".0")))),
                base);
    }

    /**
     * COMPLETE textual exponent unfold (V10c — replaces the bounded
     * DECIMAL(38,18) cast, which silently zeroed values beyond its
     * envelope): the shortest-repr mantissa digits shift by the
     * exponent as TEXT, so any finite double prints fixed-point
     * exactly — {@code 1.3421e-08 → 0.000000013421},
     * {@code 1e+300 → 1000…000.0}. Pure never prints exponent
     * notation (H1); now neither can we, for any magnitude.
     */
    private static SqlExpr exponentUnfold(SqlExpr base) {
        SqlExpr sign = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.STARTS_WITH, base,
                        new SqlExpr.StringLit("-")),
                new SqlExpr.StringLit("-"))),
                new SqlExpr.StringLit(""));
        // mantissa without sign, e.g. '1.3421'; its digits '13421';
        // intLen = digits before the dot; exp as an integer
        SqlExpr mant = SqlExpr.Call.of(SqlFn.REGEXP_EXTRACT, base,
                new SqlExpr.StringLit("-?([0-9]+(?:\\.[0-9]+)?)[eE]"),
                new SqlExpr.IntLit(1));
        SqlExpr digits = SqlExpr.Call.of(SqlFn.REPLACE, mant,
                new SqlExpr.StringLit("."), new SqlExpr.StringLit(""));
        SqlExpr dotPos = SqlExpr.Call.of(SqlFn.STRPOS, mant,
                new SqlExpr.StringLit("."));
        SqlExpr intLen = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, dotPos, new SqlExpr.IntLit(0)),
                SqlExpr.Call.of(SqlFn.LENGTH, mant))),
                SqlExpr.Call.of(SqlFn.MINUS, dotPos, new SqlExpr.IntLit(1)));
        // the exponent digits, NULL when there are none: H2 folds a constant
        // operand at prepare time BRANCH-BLIND (a literal golden cell), so
        // CAST('' AS INTEGER) would raise under the untaken CASE arm
        SqlExpr expText = SqlExpr.Call.of(SqlFn.REGEXP_EXTRACT,
                base, new SqlExpr.StringLit("[eE]([+-]?[0-9]+)$"),
                new SqlExpr.IntLit(1));
        SqlExpr exp = new SqlExpr.Cast(new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, expText, new SqlExpr.StringLit("")),
                new SqlExpr.NullLit())), expText), SqlType.Scalar.INTEGER);
        SqlExpr pointPos = SqlExpr.Call.of(SqlFn.PLUS, intLen, exp);
        SqlExpr dLen = SqlExpr.Call.of(SqlFn.LENGTH, digits);
        // three shapes by where the point lands
        SqlExpr tiny = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("0."),
                        zeros(SqlExpr.Call.of(SqlFn.MINUS,
                                new SqlExpr.IntLit(0), pointPos))),
                digits);
        SqlExpr huge = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT, digits,
                        zeros(SqlExpr.Call.of(SqlFn.MINUS, pointPos, dLen))),
                new SqlExpr.StringLit(".0"));
        SqlExpr mid = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT,
                        SqlExpr.Call.of(SqlFn.SUBSTRING, digits,
                                new SqlExpr.IntLit(1), pointPos),
                        new SqlExpr.StringLit(".")),
                SqlExpr.Call.of(SqlFn.SUBSTRING, digits,
                        SqlExpr.Call.of(SqlFn.PLUS, pointPos,
                                new SqlExpr.IntLit(1))));
        SqlExpr body = new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                        pointPos, new SqlExpr.IntLit(0)), tiny),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                        pointPos, dLen), huge)),
                mid);
        return SqlExpr.Call.of(SqlFn.CONCAT, sign, body);
    }

    /** {@code n} zeros (RPAD over empty; negative n yields ''). */
    private static SqlExpr zeros(SqlExpr n) {
        // rpad's length parameter binds INTEGER, not BIGINT
        return SqlExpr.Call.of(SqlFn.RPAD, new SqlExpr.StringLit(""),
                new SqlExpr.Cast(
                        SqlExpr.Call.of(SqlFn.GREATEST, n,
                                new SqlExpr.IntLit(0)),
                        SqlType.Scalar.INTEGER),
                new SqlExpr.StringLit("0"));
    }

    private static SqlExpr has(SqlExpr text, String needle) {
        return SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, text,
                        new SqlExpr.StringLit(needle)),
                new SqlExpr.IntLit(0));
    }
}
