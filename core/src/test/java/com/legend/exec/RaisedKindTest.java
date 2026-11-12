// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.exec;

import com.legend.sql.RaiseKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A PRODUCT RAISE IS TYPED (phase D's D1.1, 2026-11-12): the envelope's text a product dialect writes is a
 * {@link RaiseKind}'s name, which exec reads as the raise's kind and words in lite's own sentence; the edge's text
 * (legend-pure's sentence) and a user's {@code fail} message pass through as they are, kind-less.
 */
class RaisedKindTest {

    private static java.sql.SQLException raised(String inner) {
        return RaisedErrors.unwrapped(new java.sql.SQLException("Invalid Input Error: "
                + RaisedErrors.SENTINEL + inner + RaisedErrors.SENTINEL));
    }

    @Test
    void aProductRaiseIsItsKindWorded() {
        var r = assertInstanceOf(RaisedErrors.Positioned.class,
                raised("3:7" + RaisedErrors.POSITION_SEP + "DIVIDE_BY_ZERO"));
        assertEquals(RaiseKind.DIVIDE_BY_ZERO, r.kind());
        assertEquals("division by zero", r.getMessage());
        assertEquals(3, r.line());
        assertEquals(7, r.column());
        var plain = assertInstanceOf(RaisedErrors.Raised.class, raised("INDEX_OUT_OF_BOUNDS"));
        assertEquals(RaiseKind.INDEX_OUT_OF_BOUNDS, plain.kind());
        assertEquals("index out of bounds", RaisedErrors.unwrap("x" + RaisedErrors.SENTINEL + "INDEX_OUT_OF_BOUNDS"
                + RaisedErrors.SENTINEL));
    }

    @Test
    void anEdgeSentenceOrAUsersMessageIsAsWritten() {
        var edge = assertInstanceOf(RaisedErrors.Raised.class, raised("Cannot divide 5 by zero"));
        assertNull(edge.kind());
        assertEquals("Cannot divide 5 by zero", edge.getMessage());
    }

    @Test
    void everyKindIsWorded() {
        for (RaiseKind k : RaiseKind.values()) {
            assertEquals(k == RaiseKind.USER_FAIL ? "fail" : RaisedErrors.sentence(k), RaisedErrors.sentence(k));
        }
    }
}
