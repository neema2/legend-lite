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
            case SORT_TIES_AS_WRITTEN -> Scalars.sortTiesAsWritten(call, args);
            case EXTREME_KEEPS_THE_FOLDS_TIE -> Scalars.extremeKeepingTheFoldsTie(call, args);
            case NUMBERS_OF_THREE_TYPES -> Scalars.numbersOfThreeTypes(call, args);
        };
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
