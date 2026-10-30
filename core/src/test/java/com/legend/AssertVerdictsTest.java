// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Lean;
import com.legend.testing.Program;
import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import com.legend.test.StorelessRuntime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE ASSERT-FAMILY VERDICT ARM (Charter Clause 2c): statement-root
 * asserts adjudicate in World 1 ({@code PureAsserts}) over argument
 * values computed IN THE DATABASE. The model below carries the REAL
 * spec bodies (assertEquals.pure) — the K-arm must intercept the USER
 * call BEFORE β-inlining, or the bodies would compile into SQL (the
 * named Clause-2c violation; there is NO SQL assert rule anymore).
 */
// reads process-wide counters (CanonicalDivergence, Census) that every query bumps: runs alone if JUnit ever runs
// classes in parallel, so no other test's calls land between its reset and its assert (Bazel workplan P3-04, A9)
@org.junit.jupiter.api.parallel.Isolated
class AssertVerdictsTest {

    private static final String MODEL = """
            function meta::pure::functions::asserts::assertEquals(expected:Any[*], actual:Any[*]):Boolean[1]
            {
                if(eq($expected->size(), 1) && eq($actual->size(), 1),
                   | assertEquals($expected, $actual, '\\nexpected: %r\\nactual:   %r', [$expected->toOne(), $actual->toOne()]),
                   | assertEquals($expected, $actual, '\\nexpected: %s\\nactual:   %s', [$expected->map(x | $x->toRepresentation())->joinStrings('[', ', ', ']'), $actual->map(x | $x->toRepresentation())->joinStrings('[', ', ', ']')]));
            }
            function meta::pure::functions::asserts::assertEquals(expected:Any[*], actual:Any[*], formatString:String[1], formatArgs:Any[*]):Boolean[1]
            {
                assert(equal($expected, $actual), $formatString, $formatArgs);
            }
            function meta::pure::functions::asserts::assert(condition:Boolean[1], formatString:String[1], formatArgs:Any[*]):Boolean[1]
            {
                assert($condition, | format($formatString, $formatArgs));
            }
            Enum t::Kind { CITY, REGION }
            """;

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
        return Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), query, StorelessRuntime.RUNTIME, conn);
    }

    @Test
    @DisplayName("assertEquals: World-1 verdict over DB-computed sides")
    void assertEqualsVerdict() throws Exception {
        assertEquals(Boolean.TRUE, run(
                "{|assertEquals('a', ['a','b']->head())}").one());
        // the World-1 comparisons the SQL world once needed hacks for:
        // empties, [x] vs x, mixed trees — all just work host-side
        assertEquals(Boolean.TRUE, run(
                "{|assertEquals([], [1,2]->filter(x|$x > 5))}").one());
        assertEquals(Boolean.TRUE, run(
                "{|assertEquals([1,2], [1,2])}").one());
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assertEquals('a', ['a','b']->last())}"));
        assertTrue(e.getMessage().contains("expected: 'a'"),
                e.getMessage());
    }

    @Test
    @DisplayName("native assert: condition computes in the database, verdict host-side")
    void nativeAssertVerdict() throws Exception {
        assertEquals(Boolean.TRUE, run(
                "{|assert((1 + 1) == 2)}").one());
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assert((1 + 1) == 3)}"));
        assertEquals("Assert failed", e.getMessage());
    }

    @Test
    @DisplayName("assertInstanceOf: RUNTIME carrier kind up the m3 value lattice")
    void assertInstanceOfVerdict() throws Exception {
        assertEquals(Boolean.TRUE, run(
                "{|assertInstanceOf(1 + 1, Integer)}").one());
        assertEquals(Boolean.TRUE, run(
                "{|assertInstanceOf(1.5, Number)}").one());
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assertInstanceOf('x', Integer)}"));
        assertTrue(e.getMessage().contains(
                "to be an instance of Integer, actual: String"),
                e.getMessage());
    }

    @Test
    @DisplayName("assertTdsEquivalent: the GRID verdict — relations execute in DB, cells zip host-side")
    void assertTdsEquivalentVerdict() throws Exception {
        assertEquals(Boolean.TRUE, run(
                "{|assertTdsEquivalent(#TDS\n"
                + "  id, v\n  1, 2.00\n  2, 3.001\n#, #TDS\n"
                + "  id, v\n  1, 2.0\n  2, 3.0\n#, 0.01)}").one());
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assertTdsEquivalent(#TDS\n  id\n  1\n#, #TDS\n"
                + "  id\n  2\n#, 0.01)}"));
        assertTrue(e.getMessage().contains("assertTdsEquivalent"),
                e.getMessage());
    }

    /** Under the agreement gate (both judges; the plan path program's step 6, 2026-10-18). */
    private static Lean.Result runBoth(String query) throws SQLException {
        return Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), query, StorelessRuntime.RUNTIME, conn,
                ExecuteOptions.NONE.withJudgeMode(ExecuteOptions.JudgeMode.BOTH));
    }

    @Test
    @DisplayName("enum under an Any stamp: both judges agree (the database judge frames the untyped side by the"
            + " pair's one enumeration) — never a fabricated inequality")
    void enumUnderAnyDeclinesToHost() throws Exception {
        // 2026-10-18 (step 6): the byte channel that rode the side query is retired; the agreement gate judges
        // the pair twice and the two verdicts must agree
        long judged = com.legend.exec.CanonicalDivergence.sqlJudgedInDatabaseCount();
        assertEquals(Boolean.TRUE, (runBoth(
                "{|assertEquals(t::Kind.CITY, t::Kind.CITY->cast(@Any))}"))
                .value());
        assertEquals(judged + 1, com.legend.exec.CanonicalDivergence.sqlJudgedInDatabaseCount(),
                "enum-under-Any pair must be JUDGED by the database judge, in agreement with the host: "
                        + com.legend.exec.CanonicalDivergence.summary());
    }

    @Test
    @DisplayName("quantified verdict: map(f|assert(pred)) vectorizes in SQL, judges host-side")
    void quantifiedVerdict() throws Exception {
        assertEquals(Boolean.TRUE, run(
                "{|[1,2,3]->map(i|assert(['1','2','3']->contains($i->toString()),"
                + " 'element missing'))}").one());
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|[1,2,9]->map(i|assert(['1','2','3']->contains($i->toString()),"
                + " 'element missing'))}"));
        assertEquals("element missing", e.getMessage());
    }
}
