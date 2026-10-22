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
 * parity cases (IqParityTest's everyRunPlansForSnowflakeAsForDuckDb: a month name's non-capturing groups).
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

    @Test
    void aDividendOfLowerPrecedenceKeepsItsParentheses() {
        SqlExpr sum = SqlExpr.Call.of(SqlFn.PLUS, new SqlExpr.IntLit(7), new SqlExpr.IntLit(8));
        SqlExpr times = SqlExpr.Call.of(SqlFn.TIMES, new SqlExpr.IntLit(7), new SqlExpr.IntLit(8));
        String overSum = rendered(SqlExpr.Call.of(SqlFn.INT_DIVIDE, sum, new SqlExpr.IntLit(2)));
        assertTrue(overSum.contains("CAST((7 + 8 - MOD(7 + 8, NULLIF(2, 0))) / NULLIF(2, 0) AS BIGINT)"), overSum);
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
}
