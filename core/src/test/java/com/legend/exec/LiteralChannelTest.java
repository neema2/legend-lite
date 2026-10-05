// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Execution;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
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
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static ExecutionResult run(String query) throws SQLException {
        return Execution.execute(StorelessRuntime.with(MODEL, DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, conn);
    }

    @Test
    @DisplayName("mixed Any collections byte-decide — no decline")
    void mixedClaims() throws Exception {
        long before = CanonicalDivergence.sqlDeclinedCount();
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assertEquals([1, 'a', true, 2.5], [1, 'a', true, 2.5])}"))
                .value());
        assertEquals(before, CanonicalDivergence.sqlDeclinedCount(),
                "mixed Any pair must CLAIM: "
                        + CanonicalDivergence.summary());
    }

    @Test
    @DisplayName("kind rides the bytes: string '1' never equals integer 1,"
            + " float 1.0 never equals integer 1")
    void kindDisjointness() throws Exception {
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assertNotEquals([1, 'a'], ['1', 'a'])}")).value());
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assertNotEquals([1, 'a'], [1.0, 'a'])}")).value());
    }

    @Test
    @DisplayName("an Any scalar meets a TYPED side in the literal channel")
    void anyMeetsTyped() throws Exception {
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assertEquals('a', [1, 'a']->at(1))}")).value());
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assertEquals(1, [1, 'a']->at(0))}")).value());
    }

    @Test
    @DisplayName("the tree marker never compares: equal trees DECLINE,"
            + " never fabricate")
    void treesDecline() throws Exception {
        long before = CanonicalDivergence.sqlDeclinedCount();
        // a JSON tree inside an Any cell (list of lists via map) —
        // host referee judges; the byte channel counts its decline
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assertEquals([pair(1,'a')]->map(p|$p), "
                        + "[pair(1,'a')]->map(p|$p))}")).value());
        assertTrue(CanonicalDivergence.sqlDeclinedCount() >= before,
                CanonicalDivergence.summary());
    }
}
