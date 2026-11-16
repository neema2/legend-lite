// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.values.PureDateLiteral;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PRESENTER'S PRINT OF A VALUE (Phase 3 of docs/records/total-wall-edge-program-2026-11-16.md, 2026-11-16):
 * {@link Presenter#legendPure} is legend-pure's {@code toString} of a typed value, the text a raise sentence's fixed
 * operand and a failed verdict's message carry at the edge -- the same rules the edge's writers already used, now
 * reachable by value and type alone.
 */
class PresenterTest {

    @Test
    void aFloatIsLegendPuresPrint_shortestDigitsAPointAlwaysNoExponent() {
        assertEquals("1.0", Presenter.legendPure(1.0, "Float"));
        assertEquals("-1.5", Presenter.legendPure(-1.5, "Float"));
        assertEquals("0.0001", Presenter.legendPure(1e-4, "Float"));
        assertEquals("0.000000013421", Presenter.legendPure(0.000000013421, "Float"));
        assertEquals("1500000000000000.0", Presenter.legendPure(1.5e15, "Float"));
        assertEquals("NaN", Presenter.legendPure(Double.NaN, "Float"));
        // a Float the database holds exactly (a decimal slot under a Float column): its digits, a point always
        assertEquals("3.0", Presenter.legendPure(new BigDecimal("3.000"), "Float"));
    }

    @Test
    void theOtherPrimitivesAreTheirOwnText() {
        assertEquals("42", Presenter.legendPure(42L, "Integer"));
        assertEquals("18446744073709551614", Presenter.legendPure(new BigInteger("18446744073709551614"), "Integer"));
        assertEquals("1.10", Presenter.legendPure(new BigDecimal("1.10"), "Decimal"));
        assertEquals("true", Presenter.legendPure(Boolean.TRUE, "Boolean"));
        assertEquals("a'b", Presenter.legendPure("a'b", "String"));
    }

    @Test
    void aDateIsItsWrittenPrecision_plusZeroFromAMinuteOn() {
        assertEquals("2024-01-02", Presenter.legendPure(PureDateLiteral.parse("2024-01-02"), "StrictDate"));
        assertEquals("2024-01-02T10:30+0000", Presenter.legendPure(PureDateLiteral.parse("2024-01-02T10:30"),
                "DateTime"));
        assertEquals("2024-01-02T10:30:00.5+0000", Presenter.legendPure(PureDateLiteral.parse("2024-01-02T10:30:00.5"),
                "DateTime"));
    }

    /** Every operand type D's Phase 2 passes at planning (RaiseKind's typed operands: Integer, Float, Decimal, String,
     *  StrictDate, DateTime), each printed by its own name. */
    @Test
    void everyRaiseOperandTypeIsPrinted() {
        assertEquals("-7", Presenter.legendPure(-7L, "Integer"));
        assertEquals("-1.0", Presenter.legendPure(-1.0, "Float"));
        assertEquals("3.14", Presenter.legendPure(new BigDecimal("3.14"), "Decimal"));
        assertEquals("abc", Presenter.legendPure("abc", "String"));
        assertEquals("2024-02-29", Presenter.legendPure(PureDateLiteral.parse("2024-02-29"), "StrictDate"));
        assertEquals("2024-02-29T23:59:59+0000", Presenter.legendPure(PureDateLiteral.parse("2024-02-29T23:59:59"),
                "DateTime"));
    }

    @Test
    void anUnknownTypeAValueNotItsTypesAndANullAreRefusedByName() {
        IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
                () -> Presenter.legendPure(java.util.List.of(1L), "List<Integer>"));
        assertTrue(unknown.getMessage().contains("legend-pure's print of a List<Integer> is not defined"));
        IllegalArgumentException mismatch = assertThrows(IllegalArgumentException.class,
                () -> Presenter.legendPure("1", "Integer"));
        assertTrue(mismatch.getMessage().contains("is not a Integer"));
        assertThrows(IllegalArgumentException.class, () -> Presenter.legendPure(1.0, "Decimal"));
        assertThrows(IllegalArgumentException.class, () -> Presenter.legendPure(null, "String"));
    }

    @Test
    void anEmptyTdsCellIsLegendEnginesTdsNull() {
        assertEquals("TDSNull", Presenter.TDS_NULL);
    }
}
