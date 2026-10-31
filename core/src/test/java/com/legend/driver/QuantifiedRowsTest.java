// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.database.Databases;
import com.legend.error.AssertFailed;
import com.legend.exec.Census;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Program;
import com.legend.test.StorelessRuntime;

import java.sql.Connection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A quantified assert under the vector contract, judged by the host from ONE read (the seam program's step after
 * S2f, 2026-10-30): the zip's rows read once and each judged as the element arm judges it, the same decision and
 * the same message as the unroll gave; a zip arm the database answers is never fetched into Java and unrolled.
 */
class QuantifiedRowsTest {
    private static final String MODEL = "###Pure\nClass t::Marker {}\n";
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        conn = TestBackends.platform().open();
    }

    @AfterAll
    static void tearDown() throws Exception {
        conn.close();
    }

    private static Object run(String body) throws Exception {
        return Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), body, StorelessRuntime.RUNTIME, conn)
                .value();
    }

    @Test
    @DisplayName("assertEqWithinTolerance over a zip: every row within the delta holds; the first outside fails with the arm's own message")
    void toleranceRows() throws Exception {
        long before = Census.count(Census.Key.SQL_ROUND_TRIPS);
        assertEquals(Boolean.TRUE, run("{|[1.5, 2.5]->zip([1.5004, 2.4996])"
                + "->forAll(p | assertEqWithinTolerance($p.first->cast(@Float), $p.second->cast(@Float), 0.001))}"));
        AssertFailed e = assertThrows(AssertFailed.class, () -> run("{|[1.5, 2.5]->zip([1.5, 2.6])"
                + "->forAll(p | assertEqWithinTolerance($p.first, $p.second, 0.001))}"));
        assertTrue(e.getMessage().contains("expected: 2.5") && e.getMessage().contains("actual:   2.6"),
                e.getMessage());
    }

    @Test
    @DisplayName("assertEquals over a zip: each row judged as the element arm judges a pair")
    void equalsRows() throws Exception {
        assertEquals(Boolean.TRUE, run("{|[1, 2, 3]->zip([1, 2, 3])->forAll(p | assertEquals($p.first, $p.second))}"));
        AssertFailed e = assertThrows(AssertFailed.class,
                () -> run("{|['a', 'b']->zip(['a', 'c'])->forAll(p | assertEquals($p.first, $p.second))}"));
        assertTrue(e.getMessage().contains("expected: 'b'") && e.getMessage().contains("actual:   'c'"),
                e.getMessage());
    }

    @Test
    @DisplayName("assert over a zip: the predicate vector the database computes, every row true")
    void predicateRows() throws Exception {
        assertEquals(Boolean.TRUE, run("{|[1, 2]->zip([2, 3])->forAll(p | assert($p.first < $p.second, 'm'))}"));
        AssertFailed e = assertThrows(AssertFailed.class,
                () -> run("{|[1, 2]->zip([2, 1])->forAll(p | assert($p.first < $p.second, 'm'))}"));
        assertEquals("Assert failed", e.getMessage());
    }

    @Test
    @DisplayName("an argument that is neither a row's field nor a literal is refused by name, never evaluated here")
    void otherArgumentsRefused() {
        com.legend.error.NotImplementedException e = assertThrows(com.legend.error.NotImplementedException.class,
                () -> run("{|[1, 2]->zip([1, 2])->forAll(p | assertEquals($p.first + 1, $p.second + 1))}"));
        assertTrue(e.getMessage().contains("the host judge evaluates nothing"), e.getMessage());
    }
}
