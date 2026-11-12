// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.exec;

import com.legend.error.ErrorKind;
import com.legend.sql.RaiseKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A PRODUCT RAISE IS TYPED (phase D's D1.1, 2026-11-12): the envelope's text a product dialect writes is a
 * {@link RaiseKind}'s name, which exec reads as the raise's kind and nothing more; the error layer spells it
 * ({@link ErrorKind#sentence}). The edge's text (legend-pure's sentence) and a user's {@code fail} message pass through
 * as they are, kind-less.
 */
class RaisedKindTest {

    private static java.sql.SQLException failure(String inner) {
        return new java.sql.SQLException("Invalid Input Error: " + RaisedErrors.SENTINEL + inner + RaisedErrors.SENTINEL);
    }

    @Test
    void aProductRaiseIsItsKind() {
        var r = assertInstanceOf(RaisedErrors.Positioned.class,
                RaisedErrors.unwrapped(failure("3:7" + RaisedErrors.POSITION_SEP + "DIVIDE_BY_ZERO")));
        assertEquals(RaiseKind.DIVIDE_BY_ZERO, r.kind());
        assertEquals(3, r.line());
        assertEquals(7, r.column());
        var error = RaisedErrors.dataError(failure("INDEX_OUT_OF_BOUNDS"));
        assertEquals(ErrorKind.INDEX_OUT_OF_BOUNDS, error.kind());
        assertEquals("index out of bounds", error.getMessage());
        assertEquals(ErrorKind.DIVIDE_BY_ZERO, RaisedErrors.dataError("x" + RaisedErrors.SENTINEL + "DIVIDE_BY_ZERO"
                + RaisedErrors.SENTINEL, new RuntimeException()).kind());
    }

    @Test
    void anEdgeSentenceOrAUsersMessageIsAsWritten() {
        var edge = assertInstanceOf(RaisedErrors.Raised.class, RaisedErrors.unwrapped(failure("Cannot divide 5 by zero")));
        assertNull(edge.kind());
        assertEquals("Cannot divide 5 by zero", edge.getMessage());
        var error = RaisedErrors.dataError(failure("my own words"));
        assertNull(error.kind());
        assertEquals("my own words", error.getMessage());
    }
}
