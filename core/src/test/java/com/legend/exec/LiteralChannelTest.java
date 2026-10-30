// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.judge.CanonicalDivergence;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F10 v1 — THE LITERAL CHANNEL: Any-involving pairs byte-compare in
 * pure's OWN literal spellings (six disjoint forms carry kind in the
 * bytes), dispatched on the JSON carrier's runtime type IN THE
 * DATABASE. Mixed collections stop declining; string '1' and integer 1
 * can never byte-collide; an Any scalar meets a typed side in the
 * typed side's appended literal candidate.
 */
// reads process-wide counters (CanonicalDivergence, Census) that every query bumps: runs alone if JUnit ever runs
// classes in parallel, so no other test's calls land between its reset and its assert (Bazel workplan P3-04, A9)
@org.junit.jupiter.api.parallel.Isolated
class LiteralChannelTest {

    private static final String MODEL = """
            function meta::pure::functions::asserts::assertEquals(expected:Any[*], actual:Any[*]):Boolean[1]
            {
                assert(equal($expected, $actual), 'assert failed');
            }
            function meta::pure::functions::asserts::assertNotEquals(expected:Any[*], actual:Any[*]):Boolean[1]
            {
                assert(!equal($expected, $actual), 'both sides are equal');
            }
            function meta::pure::functions::asserts::assert(condition:Boolean[1], message:String[1]):Boolean[1]
            {
                assert($condition, | $message);
            }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static Lean.Result run(String query) throws SQLException {
        return Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), query, StorelessRuntime.RUNTIME, conn);
    }

    /** Under the agreement gate (both judges; the plan path program's step 6, 2026-10-18). */
    private static Lean.Result runBoth(String query) throws SQLException {
        return Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), query, StorelessRuntime.RUNTIME, conn,
                com.legend.ExecuteOptions.NONE.withJudgeMode(
                        com.legend.ExecuteOptions.JudgeMode.BOTH));
    }

    @Test
    @DisplayName("mixed Any collections are judged by both judges, in agreement — the database judge never declines")
    void mixedClaims() throws Exception {
        // 2026-10-18 (step 6): the byte channel that rode the side query is retired; the database judge is the
        // referee, and this pair is one it JUDGES (the literal channel's claim), never names unjudged
        long judged = CanonicalDivergence.sqlJudgedInDatabaseCount();
        long unjudged = CanonicalDivergence.sqlUnjudgedCount();
        assertEquals(Boolean.TRUE, (runBoth(
                "{|assertEquals([1, 'a', true, 2.5], [1, 'a', true, 2.5])}"))
                .value());
        assertEquals(judged + 1, CanonicalDivergence.sqlJudgedInDatabaseCount(),
                "mixed Any pair must be JUDGED by the database judge: " + CanonicalDivergence.summary());
        assertEquals(unjudged, CanonicalDivergence.sqlUnjudgedCount(),
                "mixed Any pair must CLAIM: " + CanonicalDivergence.summary());
    }

    @Test
    @DisplayName("kind rides the bytes: string '1' never equals integer 1,"
            + " float 1.0 never equals integer 1")
    void kindDisjointness() throws Exception {
        assertEquals(Boolean.TRUE, run(
                "{|assertNotEquals([1, 'a'], ['1', 'a'])}").one());
        assertEquals(Boolean.TRUE, run(
                "{|assertNotEquals([1, 'a'], [1.0, 'a'])}").one());
    }

    @Test
    @DisplayName("an Any scalar meets a TYPED side in the literal channel")
    void anyMeetsTyped() throws Exception {
        assertEquals(Boolean.TRUE, run(
                "{|assertEquals('a', [1, 'a']->at(1))}").one());
        assertEquals(Boolean.TRUE, run(
                "{|assertEquals(1, [1, 'a']->at(0))}").one());
    }

    @Test
    @DisplayName("the tree marker never compares: equal trees DECLINE,"
            + " never fabricate")
    void treesDecline() throws Exception {
        long before = CanonicalDivergence.sqlDeclinedCount();
        // a JSON tree inside an Any cell (list of lists via map) —
        // host referee judges; the byte channel counts its decline
        assertEquals(Boolean.TRUE, run(
                "{|assertEquals([pair(1,'a')]->map(p|$p), "
                        + "[pair(1,'a')]->map(p|$p))}").one());
        assertTrue(CanonicalDivergence.sqlDeclinedCount() >= before,
                CanonicalDivergence.summary());
    }
}
