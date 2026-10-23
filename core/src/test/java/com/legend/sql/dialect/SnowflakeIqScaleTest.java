// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snowflake's spellings the IQ scale program fixed (docs/IQ_SCALE_PROGRAM_2026_10_22.md, P1), each found before any
 * account was reached: by landing 5's review (an integer quotient) and by the plan census of the {@code #IQ{...}#}
 * parity cases (IqParityTest's everyRunPlansForTheCloudTargetAsForThePlatform: a month name's non-capturing groups).
 */
class SnowflakeIqScaleTest {

    private static String rendered(SqlExpr e) {
        return new Snowflake().render(SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(
                new SqlSelect.Projection(e, "q", null))));
    }

    /** Snowflake rounds an integer quotient to six places before a TRUNC sees it (999999999 / 1000000000 is
     *  1.000000), so two integers divide as the dividend less its remainder over the divisor. */
    @Test
    void twoIntegersDivideByTheirRemainderNotARoundedQuotient() {
        String sql = rendered(SqlExpr.Call.of(SqlFn.INT_DIVIDE, new SqlExpr.IntLit(999999999),
                new SqlExpr.IntLit(1000000000)));
        assertTrue(sql.contains("CAST((999999999 - MOD(999999999, NULLIF(1000000000, 0))) / NULLIF(1000000000, 0)"
                + " AS BIGINT)"), sql);
        assertFalse(sql.contains("TRUNC("), sql);
    }

    /** Over a literal of at most a million, a quotient's fraction (at most 0.999999) survives the six places: TRUNC
     *  is exact, and the dividend is written once, so nested divisions grow as the dividend does, not three times a
     *  level (an independent review, 2026-10-23: eight divisions by 2 were 843,095 characters). */
    @Test
    void aDivisorOfAtMostAMillionTruncatesTheQuotientWritingTheDividendOnce() {
        SqlExpr x = new SqlExpr.IntLit(999999);
        for (long d : new long[] {1_000_000, -1_000_000, 7, -1}) {
            String sql = rendered(SqlExpr.Call.of(SqlFn.INT_DIVIDE, x, new SqlExpr.IntLit(d)));
            assertTrue(sql.contains("CAST(TRUNC(999999 / " + d + ") AS BIGINT)"), sql);
        }
        for (long d : new long[] {1_000_001, -1_000_001, 0, Long.MIN_VALUE}) {
            String sql = rendered(SqlExpr.Call.of(SqlFn.INT_DIVIDE, x, new SqlExpr.IntLit(d)));
            assertTrue(sql.contains("MOD(999999, NULLIF("), sql);
        }
        SqlExpr nested = new SqlExpr.IntLit(7);
        for (int i = 0; i < 12; i++) {
            nested = SqlExpr.Call.of(SqlFn.INT_DIVIDE, nested, new SqlExpr.IntLit(2));
        }
        String sql = rendered(nested);
        assertTrue(sql.length() < 1_000, sql.length() + " characters");
    }

    @Test
    void aDividendOfLowerPrecedenceKeepsItsParentheses() {
        SqlExpr sum = SqlExpr.Call.of(SqlFn.PLUS, new SqlExpr.IntLit(7), new SqlExpr.IntLit(8));
        SqlExpr times = SqlExpr.Call.of(SqlFn.TIMES, new SqlExpr.IntLit(7), new SqlExpr.IntLit(8));
        String overSum = rendered(SqlExpr.Call.of(SqlFn.INT_DIVIDE, sum, new SqlExpr.IntLit(2_000_000)));
        assertTrue(overSum.contains("CAST((7 + 8 - MOD(7 + 8, NULLIF(2000000, 0))) / NULLIF(2000000, 0) AS BIGINT)"),
                overSum);
        String overSumByTwo = rendered(SqlExpr.Call.of(SqlFn.INT_DIVIDE, sum, new SqlExpr.IntLit(2)));
        assertTrue(overSumByTwo.contains("CAST(TRUNC((7 + 8) / 2) AS BIGINT)"), overSumByTwo);
        String overProduct = rendered(SqlExpr.Call.of(SqlFn.INT_DIVIDE, times, sum));
        assertTrue(overProduct.contains("CAST((7 * 8 - MOD(7 * 8, NULLIF(7 + 8, 0))) / NULLIF(7 + 8, 0) AS BIGINT)"),
                overProduct);
    }

    /** A replacement reading groups of a pattern with non-capturing groups (the IQ island's month names): the groups
     *  made plain, Snowflake's POSIX having no {@code (?:}, and the replacement's references renumbered. */
    @Test
    void aReplacementReadsTheGroupsANonCapturingGroupRenumbers() {
        String sql = rendered(SqlExpr.Call.of(SqlFn.REGEXP_REPLACE, new SqlExpr.StringLit("1 dece 2026"),
                new SqlExpr.StringLit("(^|[^a-z])dec(?:e(?:m(?:b(?:e(?:r)?)?)?)?)?([^a-z]|$)"),
                new SqlExpr.StringLit("\\1M12\\2")));
        assertTrue(sql.contains("'(^|[^a-z])dec(e(m(b(e(r)?)?)?)?)?([^a-z]|$)'"), sql);
        // a backslash in a Snowflake string is written twice
        assertTrue(sql.contains("'\\\\1M12\\\\7'"), sql);
        assertFalse(sql.contains("(?:"), sql);
    }

    /** A group read past \9 once renumbered is refused by name: Snowflake reads \1 to \9 only. */
    @Test
    void aReferencePastNineOnceRenumberedIsRefused() {
        SqlExpr call = SqlExpr.Call.of(SqlFn.REGEXP_REPLACE, new SqlExpr.StringLit("abc"),
                new SqlExpr.StringLit("(?:a)(?:b)(?:c)(?:d)(?:e)(?:f)(?:g)(?:h)(?:i)(x)"),
                new SqlExpr.StringLit("\\1"));
        DialectCapability refusal = assertThrows(DialectCapability.class, () -> rendered(call));
        assertTrue(refusal.getMessage().contains("\\1 to \\9"), refusal.getMessage());
    }

    /** An extraction of a literal group of such a pattern reads the group's new number. */
    @Test
    void anExtractionReadsItsGroupsNewNumber() {
        String sql = rendered(SqlExpr.Call.of(SqlFn.REGEXP_EXTRACT, new SqlExpr.StringLit("ab"),
                new SqlExpr.StringLit("(?:a)(b)"), new SqlExpr.IntLit(1)));
        assertTrue(sql.contains("REGEXP_SUBSTR('ab', '(a)(b)', 1, 1, 'ce', 2)"), sql);
    }

    /** A group the pattern does not open is refused by name: DuckDB answers '' (an extraction) or the subject (a
     *  replacement), and the plain pattern may open it (an independent review, 2026-10-23). */
    @Test
    void aGroupThePatternDoesNotOpenIsRefused() {
        for (SqlExpr call : List.of(
                SqlExpr.Call.of(SqlFn.REGEXP_EXTRACT, new SqlExpr.StringLit("xab"), new SqlExpr.StringLit("(?:a)b"),
                        new SqlExpr.IntLit(1)),
                SqlExpr.Call.of(SqlFn.REGEXP_EXTRACT, new SqlExpr.StringLit("xab"), new SqlExpr.StringLit("(?:a)(b)"),
                        new SqlExpr.IntLit(2)),
                SqlExpr.Call.of(SqlFn.REGEXP_REPLACE, new SqlExpr.StringLit("xab"), new SqlExpr.StringLit("(?:a)(b)"),
                        new SqlExpr.StringLit("\\2")))) {
            DialectCapability refusal = assertThrows(DialectCapability.class, () -> rendered(call));
            assertTrue(refusal.getMessage().contains("does not open"), refusal.getMessage());
        }
    }

    /** A bracket expression's '(' opens no group: one whose ']' comes first, or after a POSIX class, is read to its
     *  real end (an independent review, 2026-10-23: the '(' of [](] was counted, \1 renumbered to the wrong group). */
    @Test
    void aBracketExpressionsParenthesisOpensNoGroup() {
        for (String cls : List.of("[](]", "[^](]", "[[:alpha:](]")) {
            String sql = rendered(SqlExpr.Call.of(SqlFn.REGEXP_REPLACE, new SqlExpr.StringLit("x(ab"),
                    new SqlExpr.StringLit(cls + "(?:a)(b)"), new SqlExpr.StringLit("<\\1>")));
            assertTrue(sql.contains("'" + cls + "(a)(b)'") && sql.contains("'<\\\\2>'"), sql);
        }
    }

    /** A '(?:' that opens no group is left as written: escaped (an optional '(' then ':') or in a bracket expression
     *  (the replace of every '(?:' dropped their '?' and ':'). */
    @Test
    void aNonCapturingSpellingThatOpensNoGroupIsKept() {
        for (String pattern : List.of("\\(?:x", "[(?:]x", "[]?(]?:x")) {
            String sql = rendered(SqlExpr.Call.of(SqlFn.REGEXP_EXTRACT, new SqlExpr.StringLit("a(:x"),
                    new SqlExpr.StringLit(pattern)));
            assertTrue(sql.contains("'" + pattern.replace("\\", "\\\\") + "'"), sql);
        }
        assertTrue(Snowflake.live("[]a](")[4] && !Snowflake.live("[]a](")[2] && !Snowflake.live("\\(")[1]);
    }
}
