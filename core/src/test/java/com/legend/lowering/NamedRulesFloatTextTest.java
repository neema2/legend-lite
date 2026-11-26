// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE DIGITS A FLOAT LITERAL IS WRITTEN AS UNDER {@link NamedRule#FLOAT_TEXT_BY_CAST} (the edition program's step 8,
 * 2026-11-26): the exact numeric of the double's shortest digits, laid out plain with a point, so a database's cast
 * prints the digits as written. The same text lite's writers carry a Float as
 * ({@code result.CellText.carriedFloat}, which the lowering does not reach): held equal here, value by value. (A
 * negative zero is the one double an exact numeric cannot hold: its digits are a zero's, as they were when the edge's
 * pass wrote the literal through the same numeric.)
 */
class NamedRulesFloatTextTest {

    @Test
    void aLiteralsDigitsAreTheWritersTextOfTheFloat() {
        double[] values = {1.5, 2.0, 17.0, 0.1, 0.30000000000000004, 1.3421e-8, 1.3421e8, 1.0e-4, 1.0e-5, 1.0e7, 1.0e20,
                123456789.12345679, 9007199254740993.0, 4.9e-324, 1.7976931348623157e308, -2.5e16, -0.0001, 0.0,
                100.0, 1234.5678, 2.5e-17};
        for (double d : values) {
            assertEquals(com.legend.result.CellText.carriedFloat(d), NamedRules.shortestDigits(d).toPlainString(),
                    Double.toString(d));
        }
    }

    @Test
    void theDigitsReadBackAsTheSameDouble_andHoldNoZeroThatIsNoDigit() {
        assertEquals("0.000000013421", NamedRules.shortestDigits(1.3421e-8).toPlainString());
        assertEquals("134210000.0", NamedRules.shortestDigits(1.3421e8).toPlainString());
        assertEquals("0.0001", NamedRules.shortestDigits(1.0e-4).toPlainString());
        for (double d : new double[] {0.1 + 0.2, 1.0 / 3.0, Math.PI, 1.0e-300, 1.0e300}) {
            assertEquals(d, NamedRules.shortestDigits(d).doubleValue(), Double.toString(d));
        }
    }

    /** The rule itself (the independent review of 2026-11-26): any operand but a finite Float literal is cast as it
     *  is, whichever form the database takes (a column, a computed value; NaN and the infinities, which an exact
     *  numeric cannot hold, stay the literal they are); a finite literal is its digits, read as a double first only
     *  under {@link NamedRule#FLOAT_TEXT_BY_CAST_OF_A_DOUBLE}. */
    @Test
    void theRuleCastsItsOperandAsItIs_aFiniteLiteralByItsDigits() {
        SqlExpr column = SqlExpr.Column.of("t", "QTY", SqlType.Scalar.BIGINT, true,
                com.legend.sql.OutputCol.Origin.PHYSICAL);
        // (a literal that is no finite value exists only as a double's: the numeric a bare one would be has none)
        com.legend.sql.TypeFact aDouble = com.legend.sql.SqlTyping.typed(SqlType.Scalar.DOUBLE);
        for (SqlExpr asItIs : new SqlExpr[] {column, new SqlExpr.FloatLit(Double.NaN, aDouble),
                new SqlExpr.FloatLit(Double.POSITIVE_INFINITY, aDouble),
                new SqlExpr.FloatLit(Double.NEGATIVE_INFINITY, aDouble)}) {
            SqlExpr cast = new SqlExpr.Cast(asItIs, SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER);
            assertEquals(cast, NamedRules.floatTextByCast(asItIs, false));
            assertEquals(cast, NamedRules.floatTextByCast(asItIs, true));
        }
        SqlExpr digits = new SqlExpr.DecimalLit(new java.math.BigDecimal("0.000000013421"));
        assertEquals(new SqlExpr.Cast(digits, SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER),
                NamedRules.floatTextByCast(new SqlExpr.FloatLit(1.3421e-8), false));
        assertEquals(new SqlExpr.Cast(new SqlExpr.Cast(digits, SqlType.Scalar.DOUBLE, SqlExpr.Reason.DECLARED),
                        SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER),
                NamedRules.floatTextByCast(new SqlExpr.FloatLit(1.3421e-8), true));
    }
}
