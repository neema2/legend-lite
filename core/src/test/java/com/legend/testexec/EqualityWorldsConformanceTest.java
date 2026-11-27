// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import com.legend.testing.Lean;
import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.StorelessRuntime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

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
    /** Lite's own rules: what these comparisons are judged by (the explicit rule, 2026-11-04). */
    private static final HostRules LITE = HostRules.LITE;


    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = TestBackends.platform().open();
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    /** World 2: {@code {|a == b}} through the whole pipeline. The SQL
     * verdict may be NULL (SQL three-valued logic) — returned as null. */
    private static @com.legend.base.Nullable Object world2(String a, String b)
            throws Exception {
        var r = Lean.run(StorelessRuntime.with("", Databases.PLATFORM), "{|" + a + " == " + b + "}", StorelessRuntime.RUNTIME, conn);
        return r.one();
    }

    private static void agree(boolean expected, String pureA, String pureB,
            Object javaA, Object javaB) throws Exception {
        assertEquals(expected, Equality.same(Equality.Typed.of(javaA), Equality.Typed.of(javaB), LITE),
                "World 1 moved: " + pureA + " == " + pureB);
        assertEquals(expected, world2(pureA, pureB),
                "World 2 moved: " + pureA + " == " + pureB);
    }

    /** A DECLARED divergence: each world pinned at its OWN verdict, the
     * reason on the record. */
    private static void diverge(boolean world1, @com.legend.base.Nullable Object world2,
            String pureA, String pureB, Object javaA, Object javaB,
            String reason) throws Exception {
        assertEquals(world1, Equality.same(Equality.Typed.of(javaA), Equality.Typed.of(javaB), LITE),
                "World 1 moved (" + reason + "): " + pureA + " == " + pureB);
        assertEquals(world2, world2(pureA, pureB),
                "World 2 moved (" + reason + "): " + pureA + " == " + pureB);
    }

    /** A comparison across types (B3 step 5): the judge's rule answers {@code world1}; the product refuses it when the
     *  query is compiled, naming the two types. */
    private static void refusedByTheProduct(boolean world1, String pureA, String pureB, Object javaA, Object javaB) {
        assertEquals(world1, Equality.same(Equality.Typed.of(javaA), Equality.Typed.of(javaB), LITE),
                "World 1 moved: " + pureA + " == " + pureB);
        com.legend.error.ModelException e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class, () -> world2(pureA, pureB));
        org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains("two values are equal only when"),
                e.getMessage());
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
        // the one equality (B3 step 5, the user's ruling of 2026-11-24): an Integer and a Float are two types, so the
        // product refuses 1 == 1.0 as a compile error (World 2); the judge's own rule says false (World 1). Until then
        // World 2 answered SQL's numeric coercion, true (legend-engine's door still does: EngineComparisons)
        refusedByTheProduct(false, "1", "1.0", 1L, 1.0d);
        // empty == empty through a runtime read: TRUE in both worlds, as Pure says. Until the closing batch of the
        // edition program's step 6 (2026-11-27) this was a declared divergence, the recorded Phase-2 residual: SQL's
        // three-valued "no value" where Pure says true. Two sides that may both be empty are now compared null-safe
        // (lowering.Equalities.emptySidesAsPure); the STATIC [] == [] arm answered true already
        agree(true, "[]->head()", "[]->head()", null, null);
        // register S20 (the user's ruling "yes on IEEE", 2026-11-05; built 2026-11-17, Phase 5b): a Float literal is the
        // double in the product, so the database adds 0.1 + 0.2 as legend-pure's compiled runtime does
        // (0.30000000000000004), and clean mode compares a Float EXACTLY (S15/S132): the two worlds agree. Until S20 the
        // literals were DECIMALs (the numeric charter's Rule 1) and World 2 answered true. The compatibility routes'
        // 2-ULP tolerance went 2026-11-24 (B3 step 1b): exact on every route, pinned by the edge's
        // EqualityCompatEdgeTest.
        agree(false, "(0.1 + 0.2)", "0.3", 0.1d + 0.2d, 0.3d);
        // X2 (VERDICT_RULE_AUDIT): in-SQL '=' is SCALE-BLIND for
        // decimals (both engine lanes lower == to SQL) while the
        // interpreted ASSERT judgment is scale-sensitive
        // (EqualityUtilities.eq: getValue().equals) — World 1 answers
        // the engine's assert-seam FALSE, World 2 SQL's TRUE
        // X1 (VERDICT_RULE_AUDIT): the old "integral×Decimal is
        // numeric in both worlds" row MIS-CITED testIntToDecimal (that
        // test is Decimal×Decimal) — the engine assert seam rules
        // cross-primitive-kind FALSE; SQL '=' coerces TRUE
        // an Integer and a Decimal are two exact numbers, equal by their value in both worlds (the user's ruling of
        // 2026-11-27, "Yes I like the rule"; from B3 step 5 until then two types: refused by the product, false by the
        // judge's rule), as an exact number and a Float still are not
        agree(true, "8", "8D", 8L, new java.math.BigDecimal("8"));
        agree(false, "8", "8.5D", 8L, new java.math.BigDecimal("8.5"));
        refusedByTheProduct(false, "8.0", "8D", 8.0d, new java.math.BigDecimal("8"));
        // register S139 (the user's ruling, 2026-11-05): a Decimal by value, so the two worlds agree -- on every route
        // since B3 step 1a (2026-11-24; until then the edge kept the engine assert seam's scale-sensitive equals)
        agree(true, "3.0d", "3.00d", new java.math.BigDecimal("3.0"), new java.math.BigDecimal("3.00"));
    }
}
