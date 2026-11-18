// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE SPEC'S OWN CASES as pins (One-Platform Plan Phase 2a; the
 * port-engine-tests-as-spec rule): success cases and EXACT failure
 * messages from {@code platform/pure/essential/tests/assertEquals.pure}
 * / {@code assertSameElements.pure} / {@code assertSize.pure}, plus the
 * adjudicated wire policies the platform owner carries.
 */
class PureAssertsTest {
    /** legend-pure's words, the compatibility edge's (C5's step B, 2026-11-17: an edge test since). */
    private static final com.legend.edge.EdgePureWords W = com.legend.edge.EdgePureWords.INSTANCE;

    /** Lite's own rules: what these comparisons are judged by (the explicit rule, 2026-11-04). */
    private static final HostRules LITE = HostRules.LITE;

    /** assertEquals' message: the product's decision, the edge's words (the wrappers left the product, J17). */
    private static String eqMsg(List<Object> e, List<Object> a, HostRules rules, com.legend.edge.EdgePureWords w) {
        return PureAsserts.holds(e, a, rules) ? null : w.equals(e, a, null, null);
    }

    private static String sizeMsg(List<Object> c, long size, com.legend.edge.EdgePureWords w) {
        return c.size() == size ? null : w.size(size, c.size());
    }

    /** assertSameElements' message: the product's decision over both sides sorted, the edge's words (census J29). */
    private static String sameMsg(List<Object> e, List<Object> a, HostRules rules,
            com.legend.edge.EdgePureWords w) {
        return PureAsserts.holds(PureAsserts.sorted(e), PureAsserts.sorted(a), rules) ? null
                : com.legend.edge.EdgeHostWords.INSTANCE.sameElements(e, a, null, null);
    }

    /** assertEq's message: the product's decision, the edge's words (census J29). */
    private static String eqOneMsg(Object e, Object a, HostRules rules, com.legend.edge.EdgePureWords w) {
        return PureAsserts.eqHolds(e, a, rules) ? null
                : com.legend.edge.EdgeHostWords.INSTANCE.notEq(e, a, null, null);
    }

    private static String tolMsg(Number e, Number a, Number delta, com.legend.edge.EdgePureWords w) {
        return PureAsserts.withinTolerance(e, a, delta) ? null : w.pair(e, null, a, null);
    }

    private static boolean same(@com.legend.base.Nullable Object e, @com.legend.base.Nullable Object a) {
        return Equality.same(Equality.Typed.of(e), Equality.Typed.of(a), LITE);
    }

    private static boolean ordered(List<Object> e, List<Object> a) {
        return Equality.ordered(Equality.Typed.all(e, null), Equality.Typed.all(a, null), LITE);
    }


    // ---- assertEquals.pure's own tests --------------------------------

    @Test
    @DisplayName("spec: testSuccessAssertEquals + WithCollections")
    void specSuccess() {
        assertNull(eqMsg(List.of(1L), List.of(1L), LITE, W));
        assertNull(eqMsg(List.of("aaa"), List.of("aaa"), LITE, W));
        assertNull(eqMsg(List.of(1L, 2L), List.of(1L, 2L), LITE, W));
        assertNull(eqMsg(
                List.of("aaa", 2L), List.of("aaa", 2L), LITE, W));
    }

    @Test
    @DisplayName("spec: testFailureAssertEquals — the exact message")
    void specFailureMessage() {
        assertEquals("\nexpected: 1\nactual:   2",
                eqMsg(List.of(1L), List.of(2L), LITE, W));
    }

    @Test
    @DisplayName("spec: testFailureAssertEqualsWithCollections — exact messages")
    void specFailureCollections() {
        assertEquals("\nexpected: [1, 3, 2]\nactual:   [2, 4, 1, 5]",
                eqMsg(List.of(1L, 3L, 2L),
                        List.of(2L, 4L, 1L, 5L), LITE, W));
        assertEquals("\nexpected: [1, 2]\nactual:   [2, 1]",
                eqMsg(List.of(1L, 2L), List.of(2L, 1L), LITE, W));
        assertEquals("\nexpected: ['aaa', 2]\nactual:   [2, 'aaa']",
                eqMsg(List.of("aaa", 2L),
                        List.of(2L, "aaa"), LITE, W));
    }

    // ---- assertSameElements.pure's own tests --------------------------

    @Test
    @DisplayName("spec: testSuccessAssertSameElements")
    void sameElementsSuccess() {
        assertNull(sameMsg(
                List.of(1L, 2L), List.of(2L, 1L), LITE, W));
        assertNull(sameMsg(
                List.of("aaa", 2L), List.of(2L, "aaa"), LITE, W));
    }

    @Test
    @DisplayName("spec: testFailureAssertSameElements — SORTED renders, number-before-string")
    void sameElementsFailureMessage() {
        assertEquals("\nexpected: [1, 2, 3]\nactual:   [1, 2, 4, 5]",
                sameMsg(List.of(1L, 3L, 2L),
                        List.of(2L, 4L, 1L, 5L), LITE, W));
        assertEquals("\nexpected: [1, 3, '2']\nactual:   [1, 4, 5, '2']",
                sameMsg(List.of(1L, 3L, "2"),
                        List.of("2", 4L, 1L, 5L), LITE, W));
    }

    // ---- assertSize.pure ----------------------------------------------

    @Test
    @DisplayName("spec: assertSize message form")
    void sizeMessage() {
        assertNull(sizeMsg(List.of(1L, 2L), 2, W));
        assertEquals("expected size: 3, actual size: 2",
                sizeMsg(List.of(1L, 2L), 3, W));
    }

    // ---- the adjudicated wire policies (documented divergences) -------

    @Test
    @DisplayName("policy: an expected present null cell is expected-direction ONLY; a written 'TDSNull' is input")
    void tdsNullSentinel() {
        // C5 step C, C-2 (2026-11-17): the expected null cell is the compiler's typed TypedNullCell; its wire token is
        // decoded to null where a side's cells are read (SideShapes, C-2's corpus fix), so Equality meets a null beside a
        // null and never the token; a written 'TDSNull' String is INPUT, a String like any other
        assertTrue(same(null, null), "a present null cell, decoded at read, beside a NULL cell");
        assertFalse(same(com.legend.sql.SqlExpr.NULL_CELL_WIRE, null), "the token never reaches Equality as a null");
        assertFalse(same("TDSNull", null), "a written 'TDSNull' is a String, never the judges' null cell");
    }

    @Test
    @DisplayName("policy: integral kinds normalize; cross-kind is false")
    void integralNormalization() {
        assertTrue(same(1L, 1));
        assertTrue(same((short) 5, 5L));
        assertFalse(same(1L, 1.0d),
                "integral vs float is CROSS-KIND — strict false");
        assertFalse(same(1L, "1"));
    }

    @Test
    @DisplayName("policy: Decimal by value in lite (S139)")
    void numericPolicies() {
        // register S139 (the user's ruling, 2026-11-05): lite compares a Decimal by value; compatibility mode's
        // scale-sensitive equals (X2) is the edge's, EqualityCompatEdgeTest
        assertTrue(same(
                new BigDecimal("1.50"), new BigDecimal("1.5")));
        assertTrue(same(
                new BigDecimal("1.5"), new BigDecimal("1.5")));
        // X1/X3: NO cross-primitive-kind equality (engine eq requires
        // the same primitive type name)
        assertFalse(same(8L, new BigDecimal("8")));
        assertFalse(same(8.0d, new BigDecimal("8.0")));
        double base = 0.1 + 0.2;   // 0.30000000000000004
        assertTrue(same(base, 0.3 + Math.ulp(0.3)),
                "within 2 ULP compares equal (dialect libm)");
        assertFalse(same(0.3d, 0.4d));
        assertFalse(same(Double.NaN, Double.NaN),
                "NaN stays strict — never lenient");
    }

    @Test
    @DisplayName("D-arc: PureDateLiteral is THE wire temporal — precision-sensitive engine equality, bridge DEAD")
    void temporalEquality() {
        var d = com.legend.values.PureDateLiteral.parse("2014-01-01");
        assertTrue(same(d,
                com.legend.values.PureDateLiteral.parse("2014-01-01")));
        // engine PureDate.equals is PRECISION-SENSITIVE: same instant,
        // different written precision -> not equal
        assertFalse(same(
                com.legend.values.PureDateLiteral.parse("2014-01-01T10:00"),
                com.legend.values.PureDateLiteral.parse("2014-01-01T10:00:00")));
        // the string-carrier bridge DIED with the cutover (partial
        // precision rides the wire natively now): string vs temporal is
        // a TYPE mismatch, false like pure
        assertFalse(same("2014-01-01", d));
        assertFalse(same(d, "2014-01-01"));
    }

    // ---- toRepresentation (the one owner) -----------------------------

    @Test
    @DisplayName("toRepresentation: spec spellings (escape, %-dates, D-suffix)")
    void representation() {
        assertEquals("'a\\'b'", W.repr("a'b", null));
        assertEquals("'a\\nb'", W.repr("a\nb", null));
        assertEquals("%2014-01-01", W.repr(
                com.legend.values.PureDateLiteral.parse("2014-01-01"), null));
        // partial precision reprs exactly as written — impossible on
        // the old java.time carrier
        assertEquals("%2014-01", W.repr(
                com.legend.values.PureDateLiteral.parse("2014-01"), null));
        assertEquals("3.14D", W.repr(new BigDecimal("3.14"), null));
        assertEquals("1", W.repr(1L, null));
        assertEquals("true", W.repr(Boolean.TRUE, null));
    }

    // ===== 2026-08-19 phase-2 deep-audit remediation pins =====

    @Test
    @DisplayName("P2-1: tolerance is EXACT arithmetic for exact kinds")
    void toleranceExactArithmetic() {
        // 44-digit Decimals whose difference exceeds delta ONLY in exact
        // arithmetic — the old double round-trip collapsed both to the
        // same double and silently passed
        BigDecimal e = new BigDecimal("1.00000000000000000000000000000000000000000001");
        BigDecimal a = new BigDecimal("1.00000000000000000000000000000000000000000003");
        assertNotNull(tolMsg(e, a,
                new BigDecimal("1E-50"), W), "exact kinds must compare exactly");
        assertNull(tolMsg(e, a,
                new BigDecimal("1E-40"), W));
        // floating sides keep double arithmetic
        assertNull(tolMsg(1.0d, 1.0d + 1e-16, 1e-13, W));
    }

    @Test
    @DisplayName("P2-2: assertSameElements sorts temporals BY INSTANT")
    void temporalSortByInstant() {
        // date-only vs datetime mixes must order by instant, not text
        Object d2 = com.legend.values.PureDateLiteral.parse("2014-01-02");
        Object t1 = com.legend.values.PureDateLiteral.parse("2014-01-01T05:00:00");
        // instant order: t1 (Jan 1 05:00) before d2 (Jan 2 00:00);
        // TEXT order would put "2014-01-01 05:00:00" vs "2014-01-02" at
        // the mercy of the space-vs-dash byte
        assertNull(sameMsg(
                Arrays.asList(t1, d2), Arrays.asList(d2, t1), LITE, W));
    }

    @Test
    @DisplayName("P2-5: assertEq — primitives compare, non-primitives are LOUD")
    void assertEqSemantics() {
        assertNull(eqOneMsg(1L, 1L, LITE, W));
        assertNotNull(eqOneMsg(1L, 2L, LITE, W));
        assertThrows(com.legend.error.NotImplementedException.class,
                () -> eqOneMsg(Map.of("a", 1L), Map.of("a", 1L), LITE, W),
                "eq is INSTANCE identity — unobservable on a value wire");
    }

    @Test
    @DisplayName("P2-4/P2-6: wire trees — nested lists get pure numeric leaves")
    void wireTreeNestedLists() {
        // a struct cell holding an array: Long-vs-Integer inside the
        // nested list must compare by VALUE (the pre-audit Map arm fell
        // through to raw Java equals — false)
        Map<String, Object> e = Map.of("xs", List.of(1L, 2L));
        Map<String, Object> a = Map.of("xs", List.of(1, 2));
        assertTrue(same(e, a));
        assertFalse(same(
                Map.of("xs", List.of(1L, 2L)), Map.of("xs", List.of(2L, 1L))));
    }

    @Test
    @DisplayName("P2-6: document compare — content equality, first-diff path")
    void documentCompare() {
        assertNull(Equality.pureJson(
                Map.of("a", new BigDecimal("1.0")),
                Map.of("a", new BigDecimal("1.00"))));
        String d = Equality.pureJson(
                Map.of("a", List.of("x", "y")),
                Map.of("a", List.of("x", "z")));
        assertTrue(d != null && d.startsWith("$.a[1] "), String.valueOf(d));
    }

    @Test
    @DisplayName("equal() over collections: ordered, arity-strict, null-elements")
    void collectionEquality() {
        assertTrue(ordered(List.of(1L, "a"), List.of(1L, "a")));
        assertFalse(ordered(List.of(1L, 2L), List.of(2L, 1L)),
                "pure equal() is ORDERED");
        assertFalse(ordered(List.of(1L), List.of(1L, 1L)));
        assertTrue(ordered(Arrays.asList((Object) null),
                Arrays.asList((Object) null)));
    }
}
