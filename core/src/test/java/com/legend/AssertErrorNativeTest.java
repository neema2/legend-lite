// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Lean;
import com.legend.testing.Program;
import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.StorelessRuntime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@code assertError} — the platform native (One-Platform Plan Phase 4).
 * The behavioral spec is legend-pure's own: the {@code test.Test}
 * functions in essential/tests/assertError.pure (ported here — the
 * port-the-spec discipline) plus interpreted {@code AssertError.java}'s
 * contract (run f in the database, catch, adjudicate message and the
 * error's source-info channel with assertError.pure:24-26's exact
 * failure spellings).
 */
class AssertErrorNativeTest {

    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = TestBackends.platform().open();
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    private static Lean.Result run(String query) throws SQLException {
        // legend-pure's own assertError.pure, ported: its messages are legend-pure's, the edge's run's -- its style and
        // its hook together (census C1, the orchestrator's ruling of 2026-11-17: one choice per run)
        return com.legend.testing.EdgeProgram.last(StorelessRuntime.with("", Databases.PLATFORM), query,
                StorelessRuntime.RUNTIME, conn, com.legend.ExecuteOptions.NONE.withStyle(com.legend.edge.PureText.UPSTREAM));
    }

    /** {@code query} under the COMPATIBILITY RULES, heard by the edge's run (K10b of the edge audit, 2026-11-17):
     *  assertError matches a raise of a planned site by legend-pure's words of it, worded by the edge from the site's
     *  record after the run (stage B's B1, 2026-11-20) -- never lite's message. */
    private static Lean.Result compat(String query) {
        return com.legend.testing.EdgeProgram.run(StorelessRuntime.with("", Databases.PLATFORM), query,
                StorelessRuntime.RUNTIME, conn, com.legend.testing.EdgeProgram.compat()).side();
    }

    // assertError.pure:36 testSimpleAssertError, verbatim body
    @Test
    @DisplayName("matching message passes (spec testSimpleAssertError)")
    void simpleAssertError() throws Exception {
        Lean.Result r = run("{|assertError(|[1,2]->at(3),"
                + "'The system is trying to get an element at offset 3"
                + " where the collection is of size 2')}");
        assertEquals(Boolean.TRUE, r.one());
    }

    @Test
    @DisplayName("message mismatch fails with the catalog's sentence, both errors quoted")
    void messageMismatch() {
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assertError(|[1,2]->at(3), 'wrong expectation')}"));
        // (item 3, 2026-11-21: one catalog, lite's words on every route, Q15 = A; until then legend-pure's)
        assertEquals("expected the error \"wrong expectation\", got \"The system is trying to get an element at offset"
                + " 3 where the collection is of size 2\"", e.getMessage());
    }

    @Test
    @DisplayName("an assert over a zip with no message fails in the catalog's words on the edge too")
    void aVectorAssertsDefaultIsTheCatalogsOnTheEdge() {
        // (item 3, 2026-11-21: one catalog, lite's words on every route, Q15 = A; until then legend-pure's): the user's R1 (2026-11-18, PR #118) gave the edge "Assert failed"
        com.legend.error.AssertFailed e = assertThrows(com.legend.error.AssertFailed.class,
                () -> compat("{|[1, 2]->zip([2, 1])->forAll(p | assert($p.first < $p.second))}"));
        assertEquals("expected true, got false", e.getMessage());
    }

    @Test
    @DisplayName("no error is itself the failure (interpreted AssertError.java)")
    void noErrorThrown() {
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assertError(|1 + 2, 'anything')}"));
        assertEquals("expected the error \"anything\", got none", e.getMessage());
    }

    // leg 2 (assertError positions): the raise emission threads the
    // raising call's NAME-token span through the provenance envelope
    // (PureSql.raise -> U+001E -> RaisedErrors.Positioned) — interpreted
    // AssertError.java:68 hands the matcher the raising expression's
    // source info the same way. In this query text 'at' begins at 1:23.
    @Test
    @DisplayName("matching line/column pass — the raise carries its source span")
    void lineColumnMatches() throws Exception {
        Lean.Result r = run("{|assertError(|[1,2]->at(3),"
                + "'The system is trying to get an element at offset 3"
                + " where the collection is of size 2', 1, 23)}");
        assertEquals(Boolean.TRUE, r.one());
    }

    @Test
    @DisplayName("line mismatch fails with the catalog's sentence")
    void lineMismatchSpelling() {
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assertError(|[1,2]->at(3),"
                + "'The system is trying to get an element at offset 3"
                + " where the collection is of size 2', 42, [])}"));
        assertEquals("expected the error at line 42, got line 1", e.getMessage());
    }

    @Test
    @DisplayName("column mismatch fails with the catalog's sentence")
    void columnMismatchSpelling() {
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assertError(|[1,2]->at(3),"
                + "'The system is trying to get an element at offset 3"
                + " where the collection is of size 2', 1, 5)}"));
        assertEquals("expected the error at column 5, got column 23", e.getMessage());
    }


    // assertError.pure:30 — the /2 overload delegates with [] line/[] col:
    // empty expectations skip the source-info checks
    @Test
    @DisplayName("empty line/column expectations skip the source-info checks")
    void emptyLineColumn() throws Exception {
        Lean.Result r = run("{|assertError(|[1,2]->at(3),"
                + "'The system is trying to get an element at offset 3"
                + " where the collection is of size 2', [], [])}");
        assertEquals(Boolean.TRUE, r.one());
    }

    // date.pure:69 testNewDateError shape — the date ctor's guards speak
    // DateFunctions.validateDay's exact spellings: too-large days name the
    // attempted y-m-d (month un-padded, MONTH-AWARE incl. leap years)
    @Test
    @DisplayName("date ctor guards: month-aware day validation, spec spellings")
    void newDateErrors() throws Exception {
        // legend-pure's words of each raise site, through the compatibility run's hearing (K10b, 2026-11-17)
        compat("{|assertError(|date(2016, 13), 'Invalid month: 13')}");
        compat("{|assertError(|date(2016, 12, 32), 'Invalid day: 2016-12-32')}");
        compat("{|assertError(|date(2016, 2, 30), 'Invalid day: 2016-2-30')}");
        compat("{|assertError(|date(2015, 2, 29), 'Invalid day: 2015-2-29')}");
        compat("{|assertError(|date(2016, 12, 31, 24), 'Invalid hour: 24')}");
        // leap-year Feb 29 is LEGAL — no error is the assert failure
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assertError(|date(2016, 2, 29), 'anything')}"));
        assertEquals("expected the error \"anything\", got none", e.getMessage());
    }

    // ---- B7 (RaisedErrors): the ONE envelope owner's spec — sentinel
    // pair extracts OUR raised text from any transport envelope; a
    // native error (no sentinel) passes through WHOLE, class and all
    // (the old broad prefix strip could launder a native error into
    // pure's expected text — that coincidence class is dead) ----

    @Test
    @DisplayName("unwrap extracts between the provenance sentinels —"
            + " prefix, suffix, or both envelopes")
    void unwrapSentinels() {
        String s = String.valueOf(com.legend.exec.RaisedErrors.SENTINEL);
        assertEquals("Cannot get hour for 2017",
                com.legend.exec.RaisedErrors.unwrap(
                        "Invalid Input Error: " + s
                        + "Cannot get hour for 2017" + s));
        assertEquals("msg", com.legend.exec.RaisedErrors.unwrap(
                s + "msg" + s + "; SQL statement: SELECT ... [45000-232]"));
        assertEquals("a\nb", com.legend.exec.RaisedErrors.unwrap(
                "Prefix: " + s + "a\nb" + s + " suffix"));
    }

    @Test
    @DisplayName("a NATIVE error (no sentinel) passes through whole —"
            + " envelope, class and all")
    void nativeErrorsUntouched() {
        assertEquals("Invalid Input Error: Cannot get hour for 2017",
                com.legend.exec.RaisedErrors.unwrap(
                        "Invalid Input Error: Cannot get hour for 2017"));
        assertEquals("Conversion Error: Could not convert string 'x' to INT64",
                com.legend.exec.RaisedErrors.unwrap(
                        "Conversion Error: Could not convert string 'x'"
                        + " to INT64"));
    }
}
