// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.error.NotImplementedException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tier-1 regression pin for audit finding E (2026-08-18): row-identifier
 * cells are PURE SOURCE spelled by the engine's {@code toRepresentation()}
 * (platform toRepresentation.pure) — backslash escapes, {@code %} dates,
 * {@code D} decimals. The old inline speller emitted
 * {@code "'" + str + "'"} with NO escaping (a quote in a cell broke the
 * generated source) and JDBC-default spellings for everything else.
 * Each case here FAILS on that speller.
 */
class PureReprTest {

    @Test
    void stringsTakePureBackslashEscapes() {
        // the spec's replace order: backslash, then quote, then newline
        assertEquals("'don\\'t'", TestDataGenerator.pureRepr("don't"));
        assertEquals("'a\\\\b'", TestDataGenerator.pureRepr("a\\b"));
        assertEquals("'x\\ny'", TestDataGenerator.pureRepr("x\ny"));
        assertEquals("'plain'", TestDataGenerator.pureRepr("plain"));
    }

    @Test
    void numbersAndBooleansSpellLikeTheEngine() {
        assertEquals("7", TestDataGenerator.pureRepr(7));
        assertEquals("7", TestDataGenerator.pureRepr(7L));
        assertEquals("1.00D",
                TestDataGenerator.pureRepr(new BigDecimal("1.00")));
        assertEquals("true", TestDataGenerator.pureRepr(Boolean.TRUE));
    }

    @Test
    void datesTakeThePercentForm() {
        // the carrier the plan runner's typed read hands (the doors, 2026-10-29: the generator reads no result set of
        // its own, so a driver's java.sql.Date or java.time day never reaches it)
        assertEquals("%2020-01-02", TestDataGenerator.pureRepr(
                com.legend.values.PureDateLiteral.fromLocalDate(java.time.LocalDate.of(2020, 1, 2))));
    }

    @Test
    void nullAndUnknownKindsWallLoudly() {
        // a NULL primary-key cell is a defect, never the text "null"
        assertThrows(NotImplementedException.class,
                () -> TestDataGenerator.pureRepr(null));
        assertThrows(NotImplementedException.class,
                () -> TestDataGenerator.pureRepr(new Object()));
    }
}
