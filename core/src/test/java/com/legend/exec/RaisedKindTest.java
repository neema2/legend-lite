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
                RaisedErrors.unwrapped(failure("3:7" + RaisedErrors.POSITION_SEP + RaiseKind.MARK + "DIVIDE_BY_ZERO")));
        assertEquals(RaiseKind.DIVIDE_BY_ZERO, r.kind());
        assertEquals(3, r.line());
        assertEquals(7, r.column());
        var error = RaisedErrors.dataError(failure(RaiseKind.MARK + "INDEX_OUT_OF_BOUNDS"));
        assertEquals(ErrorKind.INDEX_OUT_OF_BOUNDS, error.kind());
        assertEquals("index out of bounds", error.getMessage());
        assertEquals(ErrorKind.DIVIDE_BY_ZERO, RaisedErrors.dataError("x" + RaisedErrors.SENTINEL + RaiseKind.MARK
                + "DIVIDE_BY_ZERO" + RaisedErrors.SENTINEL, new RuntimeException()).kind());
    }

    /** A product raise's where, a fixed text known at planning, follows its kind's sentence (the D part 1 audit's S4,
     *  2026-11-15); a user's fail text spelled as a kind's name is the user's text, never that kind (its nit). */
    @Test
    void aRaisesWhereIsWorded_andAUsersTextIsNeverAKind() {
        var placed = RaisedErrors.dataError(failure(RaiseKind.MARK + "FRACTION_NOT_KEPT" + RaiseKind.MARK
                + "the DateTime parameter 't'"));
        assertEquals(ErrorKind.FRACTION_NOT_KEPT, placed.kind());
        assertEquals("more fraction digits than this database keeps: the DateTime parameter 't'", placed.getMessage());
        var users = RaisedErrors.dataError(failure("DIVIDE_BY_ZERO"));
        assertEquals(null, users.kind());
        assertEquals("DIVIDE_BY_ZERO", users.getMessage());
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
