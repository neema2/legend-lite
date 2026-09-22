// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE TWO-WORLDS CONFORMANCE FIXTURE (Charter Clause 2c, 2026-08-19
 * phase-2 deep audit): the same value pair judged by BOTH equality
 * worlds — World 1 ({@link PureAsserts#equalScalar}: the host
 * adjudication layer over fetched values) and World 2 (the compiler:
 * {@code a == b} lowered to SQL and executed). Every row pins BOTH
 * verdicts; a DIVERGENT row is a DECLARED divergence with its reason —
 * drift in either world is a red test, never a three-phases-later
 * discovery.
 */
class EqualityWorldsConformanceTest {

    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    /** World 2: {@code {|a == b}} through the whole pipeline. The SQL
     * verdict may be NULL (SQL three-valued logic) — returned as null. */
    private static @com.legend.base.Nullable Object world2(String a, String b)
            throws Exception {
        var r = Compiler.execute("", "{|" + a + " == " + b + "}", conn);
        return ((ExecutionResult.Scalar) r).value();
    }

    private static void agree(boolean expected, String pureA, String pureB,
            Object javaA, Object javaB) throws Exception {
        assertEquals(expected, Equality.same(Equality.Typed.of(javaA), Equality.Typed.of(javaB)),
                "World 1 moved: " + pureA + " == " + pureB);
        assertEquals(expected, world2(pureA, pureB),
                "World 2 moved: " + pureA + " == " + pureB);
    }

    /** A DECLARED divergence: each world pinned at its OWN verdict, the
     * reason on the record. */
    private static void diverge(boolean world1, @com.legend.base.Nullable Object world2,
            String pureA, String pureB, Object javaA, Object javaB,
            String reason) throws Exception {
        assertEquals(world1, Equality.same(Equality.Typed.of(javaA), Equality.Typed.of(javaB)),
                "World 1 moved (" + reason + "): " + pureA + " == " + pureB);
        assertEquals(world2, world2(pureA, pureB),
                "World 2 moved (" + reason + "): " + pureA + " == " + pureB);
    }

    @Test
    @DisplayName("shared ground: both worlds agree")
    void sharedGround() throws Exception {
        agree(true, "1", "1", 1L, 1L);
        agree(false, "1", "2", 1L, 2L);
        agree(true, "1.5", "1.5", 1.5d, 1.5d);
        agree(false, "1.5", "2.5", 1.5d, 2.5d);
        agree(true, "'a'", "'a'", "a", "a");
        agree(false, "'a'", "'b'", "a", "b");
        agree(true, "true", "true", true, true);
        agree(false, "true", "false", true, false);
        agree(true, "%2014-01-01", "%2014-01-01",
                java.sql.Date.valueOf("2014-01-01"),
                java.sql.Date.valueOf("2014-01-01"));
        agree(false, "%2014-01-01", "%2014-01-02",
                java.sql.Date.valueOf("2014-01-01"),
                java.sql.Date.valueOf("2014-01-02"));

    }

    @Test
    @DisplayName("declared divergences: each world pinned at its own verdict")
    void declaredDivergences() throws Exception {
        // SQL numeric coercion: 1 = 1.0 is TRUE in SQL (and in the
        // engine's relational execution — the frontier); pure equal is
        // cross-KIND false (integral vs float)
        diverge(false, true, "1", "1.0", 1L, 1.0d,
                "SQL numeric coercion — engine-relational parity");
        // SQL three-valued logic: empty == empty through a runtime read
        // is SQL NULL where pure says TRUE (the recorded residual; the
        // STATIC [] == [] arm answers true, this pins the RUNTIME path)
        diverge(true, null, "[]->head()", "[]->head()", null, null,
                "SQL NULL vs pure true — recorded Phase-2 residual");
        // R1 widening: the 2-ULP arithmetic leniency is World 1's
        // DECLARED numeric policy (outside the byte channel — R0);
        // World 2's IEEE compare is exact. R3's census decides
        // retire-vs-keep; a healed row tightens here.
        // NUMERIC CHARTER Rule 1 (2026-09-17): Float literals render BARE
        // (the engine's own literal processor) and the database adds them
        // in DECIMAL — 0.1 + 0.2 IS 0.3 in SQL on both backends, the engine's
        // relational verdict; the interpreter's double arithmetic
        // (0.30000000000000004) is World 1's 2-ULP policy. Both worlds answer
        // true, each for its own reason, and each is pinned.
        diverge(true, true, "(0.1 + 0.2)", "0.3", 0.1d + 0.2d, 0.3d,
                "2-ULP declared policy vs the database's DECIMAL arithmetic over bare literals");
        // X2 (VERDICT_RULE_AUDIT): in-SQL '=' is SCALE-BLIND for
        // decimals (both engine lanes lower == to SQL) while the
        // interpreted ASSERT judgment is scale-sensitive
        // (EqualityUtilities.eq: getValue().equals) — World 1 answers
        // the engine's assert-seam FALSE, World 2 SQL's TRUE
        // X1 (VERDICT_RULE_AUDIT): the old "integral×Decimal is
        // numeric in both worlds" row MIS-CITED testIntToDecimal (that
        // test is Decimal×Decimal) — the engine assert seam rules
        // cross-primitive-kind FALSE; SQL '=' coerces TRUE
        diverge(false, true, "8", "8D", 8L, new java.math.BigDecimal("8"),
                "SQL numeric coercion vs engine same-kind-only eq");
        diverge(false, true, "3.0d", "3.00d",
                new java.math.BigDecimal("3.0"),
                new java.math.BigDecimal("3.00"),
                "SQL scale-blind '=' vs engine assert-seam equals");
    }
}
