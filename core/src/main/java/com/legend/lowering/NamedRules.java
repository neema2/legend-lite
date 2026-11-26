// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;

/**
 * THE RULE EACH {@link NamedRule} NAMES: what the lowering writes for a call an edition's table maps to it
 * ({@link Scalars#lower}). One arm per name, no default: a name with no rule does not compile.
 */
final class NamedRules {

    private NamedRules() {
    }

    /** A plain decimal number's text, the pattern {@link NamedRule#TEXT_IS_PLAIN_DECIMAL} matches in full. */
    private static final String PLAIN_DECIMAL = "[-]?[0-9]+([.][0-9]+)?";

    /** {@code call} written by {@code rule} over its lowered arguments. */
    static SqlExpr lower(NamedRule rule, TypedNativeCall call, List<SqlExpr> args) {
        return switch (rule) {
            case SUBSTRING_AS_GIVEN -> new SqlExpr.Call(SqlFn.SUBSTRING_AS_GIVEN, args);
            case SUBSTR_AS_GIVEN -> substrAsGiven(args);
            case SQL_POSITION -> new SqlExpr.Call(SqlFn.STRPOS, args);
            case TRUST_ONE -> args.get(0);
            case ROUND_AS_GIVEN -> DecimalKindRules.roundAsGiven(call, args);
            case CAST_TO_BARE_DECIMAL -> new SqlExpr.Call(SqlFn.DECIMAL_RAW, args);
            case TEXT_CAST_TO_INTEGER -> new SqlExpr.Call(SqlFn.CAST_INT, args);
            case TEXT_CAST_TO_FLOAT -> new SqlExpr.Call(SqlFn.CAST_FLOAT, args);
            case TEXT_HAS_NO_CASED_LETTER -> SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.LOWER, args.get(0)),
                    SqlExpr.Call.of(SqlFn.UPPER, args.get(0)));
            case TEXT_IS_PLAIN_DECIMAL -> SqlExpr.Call.of(SqlFn.REGEXP_FULL_MATCH, args.get(0),
                    new SqlExpr.StringLit(PLAIN_DECIMAL));
            case DATE_DIFF_AS_GIVEN -> Scalars.dateDiffAsGiven(call, args);
            case SPLIT_PART_AS_GIVEN -> SqlExpr.Call.of(SqlFn.SPLIT_PART, args.get(0), args.get(1),
                    SqlExpr.Call.of(SqlFn.PLUS, args.get(2), new SqlExpr.IntLit(1)));
            case FLOAT_TEXT_BY_CAST -> floatTextByCast(args.get(0), false);
            case FLOAT_TEXT_BY_CAST_OF_A_DOUBLE -> floatTextByCast(args.get(0), true);
            case DATE_TIME_TEXT_BY_CAST -> new SqlExpr.Cast(args.get(0), com.legend.sql.SqlType.Scalar.VARCHAR,
                    SqlExpr.Reason.USER);
        };
    }

    /** The database's own cast of {@code x} to text. A finite Float literal is written as the exact numeric of its
     *  shortest digits ({@link #shortestDigits}), read as a double first where {@code asDouble}; any other operand is
     *  cast as it is held (moved here 2026-11-26 from the compatibility edge's SQL pass, {@code edge.PurePrintForms},
     *  which wrote this for every Float text of an edge plan: the edition program's step 8). */
    static SqlExpr floatTextByCast(SqlExpr x, boolean asDouble) {
        SqlExpr operand = x;
        if (x instanceof SqlExpr.FloatLit f && Double.isFinite(f.value())) {
            SqlExpr digits = new SqlExpr.DecimalLit(shortestDigits(f.value()));
            operand = asDouble ? new SqlExpr.Cast(digits, com.legend.sql.SqlType.Scalar.DOUBLE, SqlExpr.Reason.DECLARED)
                    : digits;
        }
        return new SqlExpr.Cast(operand, com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER);
    }

    /** A finite double's shortest digits that read back as the same double ({@code Double.toString}'s), as an exact
     *  numeric laid out plain: a point always, no exponent, no zero that is no digit of the value ({@code 1.5},
     *  {@code 134210000.0}, {@code 0.0001}, {@code 0.000000013421}). A fact of the program's literal, not of a row.
     *  The same text lite's writers carry a Float as ({@code result.CellText.carriedFloat}, which this library does
     *  not reach; {@code NamedRulesFloatTextTest} holds the two equal). */
    static java.math.BigDecimal shortestDigits(double d) {
        String shortest = Double.toString(d);
        if (shortest.indexOf('E') < 0) {
            return new java.math.BigDecimal(shortest);
        }
        String plain = new java.math.BigDecimal(shortest).stripTrailingZeros().toPlainString();
        return new java.math.BigDecimal(plain.contains(".") ? plain : plain + ".0");
    }

    /** SUBSTRING_AS_GIVEN over {@code start + 1} and {@code end - start}: the positions are SQL arithmetic the
     *  database computes, a literal index included (no fold here), and no pass rewrites the call. */
    private static SqlExpr substrAsGiven(List<SqlExpr> args) {
        SqlExpr start1 = SqlExpr.Call.of(SqlFn.PLUS, args.get(1), new SqlExpr.IntLit(1));
        if (args.size() == 2) {
            return new SqlExpr.Call(SqlFn.SUBSTRING_AS_GIVEN, List.of(args.get(0), start1));
        }
        return new SqlExpr.Call(SqlFn.SUBSTRING_AS_GIVEN, List.of(args.get(0), start1,
                SqlExpr.Call.of(SqlFn.MINUS, args.get(2), args.get(1))));
    }
}
