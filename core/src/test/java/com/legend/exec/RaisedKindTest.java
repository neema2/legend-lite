// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    /** A statement's site table: site 1 a division by zero, site 2 a fraction kept, placed. */
    private static final List<ExecutionPlan.Site> SITES = List.of(
            new ExecutionPlan.Site(1, RaiseKind.DIVIDE_BY_ZERO, null, "division by zero", null),
            new ExecutionPlan.Site(2, RaiseKind.FRACTION_NOT_KEPT, "the DateTime parameter 't'",
                    "more fraction digits than this database keeps: the DateTime parameter 't'", null),
            new ExecutionPlan.Site(3, RaiseKind.INDEX_OUT_OF_BOUNDS, null, "index out of bounds",
                    "The system is trying to get an element at offset 5 where the collection is of size 3"));

    @Test
    void aSitesTokenIsItsKindAndItsPlannedWords() {
        var r = assertInstanceOf(RaisedErrors.Positioned.class,
                RaisedErrors.unwrapped(failure("3:7" + RaisedErrors.POSITION_SEP + RaiseKind.TOKEN + "1"), SITES));
        assertEquals(RaiseKind.DIVIDE_BY_ZERO, r.kind());
        assertEquals(3, r.line());
        assertEquals(7, r.column());
        var error = RaisedErrors.dataErrorOf(RaisedErrors.unwrapped(failure(RaiseKind.TOKEN + "3"), SITES));
        assertEquals(ErrorKind.INDEX_OUT_OF_BOUNDS, error.kind());
        assertEquals("The system is trying to get an element at offset 5 where the collection is of size 3",
                error.getMessage(), "an edge plan's site says upstream's words, planned");
        assertEquals(ErrorKind.DIVIDE_BY_ZERO, RaisedErrors.dataError("x" + RaisedErrors.SENTINEL + RaiseKind.TOKEN + "1"
                + RaisedErrors.SENTINEL, new RuntimeException(), SITES).kind());
    }

    /** A site's where, a fixed text known at planning, follows its kind's sentence in its planned message; a user's
     *  fail text spelled as a kind's name is the user's text, never that kind. */
    @Test
    void aSitesWhereIsWorded_andAUsersTextIsNeverAKind() {
        var placed = RaisedErrors.dataErrorOf(RaisedErrors.unwrapped(failure(RaiseKind.TOKEN + "2"), SITES));
        assertEquals(ErrorKind.FRACTION_NOT_KEPT, placed.kind());
        assertEquals("more fraction digits than this database keeps: the DateTime parameter 't'", placed.getMessage());
        var users = RaisedErrors.dataErrorOf(RaisedErrors.unwrapped(failure("DIVIDE_BY_ZERO"), SITES));
        assertEquals(null, users.kind());
        assertEquals("DIVIDE_BY_ZERO", users.getMessage());
    }

    /** A token the statement does not declare is refused by name: the database reported what the plan did not plan. */
    @Test
    void anUndeclaredTokenIsRefusedByName() {
        var refused = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> RaisedErrors.unwrapped(failure(RaiseKind.TOKEN + "4"), SITES));
        assertTrue(refused.getMessage().contains("LG#4"), refused.getMessage());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> RaisedErrors.unwrapped(failure(RaiseKind.TOKEN + "1"), List.of()));
    }

    /** A user's fail text spelled as a token, unmarked, is the user's own text, never a site (the D part 1 audit's
     *  S4: the mark opens every token for this). */
    @Test
    void aUsersTextSpelledAsATokenIsNeverASite() {
        var users = RaisedErrors.dataErrorOf(RaisedErrors.unwrapped(failure("LG#1"), SITES));
        assertNull(users.kind());
        assertEquals("LG#1", users.getMessage());
    }

    /** A failure the database raised itself, no envelope of lite's, is DATABASE_ERROR: its words the database's,
     *  verbatim, and its SQLSTATE; a connection lost (SQLSTATE class 08), or a session that did not open, is
     *  CONNECTION_ERROR (phase D's Phase 2, item 4, 2026-11-16). */
    @Test
    void theDatabasesOwnFailureIsItsKindWithItsOwnWords() {
        var own = RaisedErrors.dataErrorOf(RaisedErrors.unwrapped(
                new java.sql.SQLException("Conversion Error: no such cast", "22018"), SITES));
        assertEquals(ErrorKind.DATABASE_ERROR, own.kind());
        assertEquals("Conversion Error: no such cast", own.getMessage());
        assertEquals("Conversion Error: no such cast", own.databaseMessage());
        assertEquals("22018", own.sqlState());
        var lost = RaisedErrors.dataErrorOf(new java.sql.SQLNonTransientConnectionException("connection reset", "08006"));
        assertEquals(ErrorKind.CONNECTION_ERROR, lost.kind());
        var unopened = RaisedErrors.connectionError(new java.sql.SQLException("no such database", "XX000"));
        assertEquals(ErrorKind.CONNECTION_ERROR, unopened.kind());
        assertEquals("no such database", unopened.databaseMessage());
        var users = RaisedErrors.dataErrorOf(RaisedErrors.unwrapped(failure("my own words"), SITES));
        assertNull(users.kind(), "a user's fail is lite's envelope, never the database's");
        assertNull(users.databaseMessage());
    }

    @Test
    void anEdgeSentenceOrAUsersMessageIsAsWritten() {
        var edge = assertInstanceOf(RaisedErrors.Raised.class,
                RaisedErrors.unwrapped(failure("Cannot divide 5 by zero"), SITES));
        assertNull(edge.kind());
        assertEquals("Cannot divide 5 by zero", edge.getMessage());
        var error = RaisedErrors.dataErrorOf(RaisedErrors.unwrapped(failure("my own words"), SITES));
        assertNull(error.kind());
        assertEquals("my own words", error.getMessage());
    }
}
