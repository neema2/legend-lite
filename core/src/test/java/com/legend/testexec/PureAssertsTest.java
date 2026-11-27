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

    /** Lite's own rules: what these comparisons are judged by (the explicit rule, 2026-11-04). */
    private static final HostRules LITE = HostRules.LITE;

    // Each decision is the product's (PureAsserts, Equality), the spec's own cases; each failure is a record of the one
    // catalog, lite's words on every route (item 3, 2026-11-21: until then legend-pure's words, the edge's EdgePureWords
    // and EdgeHostWords, both deleted under Q15 = A).

    /** assertEquals' failure. */
    private static String eqMsg(List<Object> e, List<Object> a, HostRules rules) {
        return PureAsserts.holds(e, a, rules) ? null : AssertRecords.sidesNotEqual(e, a, e, a, null, null, null, null).sentence();
    }

    private static String sizeMsg(List<Object> c, long size) {
        return c.size() == size ? null : AssertRecords.size(size, c.size()).sentence();
    }

    /** assertSameElements' failure: the product's decision over both sides sorted (Pure's sort), each side's values
     *  in its own order in the words. */
    private static String sameMsg(List<Object> e, List<Object> a, HostRules rules) {
        return PureAsserts.holds(sorted(e), sorted(a), rules) ? null
                : AssertRecords.sameElements(e, a, null, null).sentence();
    }

    /** assertEq's failure. */
    private static String eqOneMsg(Object e, Object a, HostRules rules) {
        return PureAsserts.eqHolds(e, null, a, null, rules) ? null
                : AssertRecords.notEq(e, a, null, null, null, null).sentence();
    }

    private static String tolMsg(Number e, Number a, Number delta) {
        return PureAsserts.withinTolerance(e, a, delta) ? null : AssertRecords.notWithin(e, a, delta).sentence();
    }

    /** Pure's sort over Any (numbers by value, then strings, then booleans, temporals by instant): Equality's order
     *  (PureAsserts.sorted until item 3's S2, which went with the edge's legend-pure words, its one caller). */
    private static List<Object> sorted(List<Object> values) {
        List<Object> out = new java.util.ArrayList<>(values.size());
        for (Equality.Typed t : Equality.sorted(Equality.Typed.all(values, null))) {
            out.add(t.value());
        }
        return out;
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
        assertNull(eqMsg(List.of(1L), List.of(1L), LITE));
        assertNull(eqMsg(List.of("aaa"), List.of("aaa"), LITE));
        assertNull(eqMsg(List.of(1L, 2L), List.of(1L, 2L), LITE));
        assertNull(eqMsg(
                List.of("aaa", 2L), List.of("aaa", 2L), LITE));
    }

    @Test
    @DisplayName("spec: testFailureAssertEquals — the exact message")
    void specFailureMessage() {
        assertEquals("expected: 1, actual: 2",
                eqMsg(List.of(1L), List.of(2L), LITE));
    }

    @Test
    @DisplayName("spec: testFailureAssertEqualsWithCollections — exact messages")
    void specFailureCollections() {
        assertEquals("expected: [1, 3, 2], actual: [2, 4, 1, 5]",
                eqMsg(List.of(1L, 3L, 2L),
                        List.of(2L, 4L, 1L, 5L), LITE));
        assertEquals("expected: [1, 2], actual: [2, 1]",
                eqMsg(List.of(1L, 2L), List.of(2L, 1L), LITE));
        assertEquals("expected: [\"aaa\", 2], actual: [2, \"aaa\"]",
                eqMsg(List.of("aaa", 2L),
                        List.of(2L, "aaa"), LITE));
    }

    // ---- assertSameElements.pure's own tests --------------------------

    @Test
    @DisplayName("spec: testSuccessAssertSameElements")
    void sameElementsSuccess() {
        assertNull(sameMsg(
                List.of(1L, 2L), List.of(2L, 1L), LITE));
        assertNull(sameMsg(
                List.of("aaa", 2L), List.of(2L, "aaa"), LITE));
    }

    @Test
    @DisplayName("equal numbers of two types written in two orders are the same elements: the sort gives each one"
            + " place on both sides (slice 6.2; they were paired as written, 3 against 3.0, and failed)")
    void sameElementsWithEqualNumbersOfTwoTypes() {
        assertNull(sameMsg(List.of(3L, 3.0d, "a"), List.of(3.0d, 3L, "a"), LITE));
        assertNull(sameMsg(List.of(3.0d, 3L, new java.math.BigDecimal("3")),
                List.of(new java.math.BigDecimal("3"), 3L, 3.0d), LITE));
        // and two types are still two values: an Integer does not stand in for a Float
        assertEquals("expected the elements [3, 3] in any order, got [3, 3.0]",
                sameMsg(List.of(3L, 3L), List.of(3L, 3.0d), LITE));
    }

    @Test
    @DisplayName("spec: testFailureAssertSameElements — lite's words, each side in its own order")
    void sameElementsFailureMessage() {
        assertEquals("expected the elements [1, 3, 2] in any order, got [2, 4, 1, 5]",
                sameMsg(List.of(1L, 3L, 2L),
                        List.of(2L, 4L, 1L, 5L), LITE));
        assertEquals("expected the elements [1, 3, \"2\"] in any order, got [\"2\", 4, 1, 5]",
                sameMsg(List.of(1L, 3L, "2"),
                        List.of("2", 4L, 1L, 5L), LITE));
    }

    // ---- assertSize.pure ----------------------------------------------

    @Test
    @DisplayName("spec: assertSize message form")
    void sizeMessage() {
        assertNull(sizeMsg(List.of(1L, 2L), 2));
        assertEquals("expected size: 3, actual size: 2",
                sizeMsg(List.of(1L, 2L), 3));
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
        // register S139 (the user's ruling, 2026-11-05): a Decimal by value, on every route since B3 step 1a (2026-11-24;
        // until then the edge's scale-sensitive equals, X2)
        assertTrue(same(
                new BigDecimal("1.50"), new BigDecimal("1.5")));
        assertTrue(same(
                new BigDecimal("1.5"), new BigDecimal("1.5")));
        // numbers are of two families (the user's ruling of 2026-11-27): an Integer and a Decimal, two exact
        // numbers, are equal by their value; an exact number never equals a Float (X1/X3 until then: no equality
        // across any two primitive kinds, as the engine's eq requires the same primitive type name)
        assertTrue(same(8L, new BigDecimal("8")));
        assertTrue(same(new BigDecimal("8.00"), 8L));
        assertFalse(same(8L, new BigDecimal("8.5")));
        assertFalse(same(8.0d, new BigDecimal("8.0")));
        assertFalse(same(8L, 8.0d));
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
        // register S399 (the user's ruling of 2026-11-22): lite's product compares a date-time by its instant, so one
        // instant at two written precisions is one value (legend-pure's PureDate.equals is precision-sensitive; lite's
        // rule on every route since B3's one rulebook, 2026-11-24); a day is never a date-time
        assertTrue(same(
                com.legend.values.PureDateLiteral.parse("2014-01-01T10:00"),
                com.legend.values.PureDateLiteral.parse("2014-01-01T10:00:00")));
        assertFalse(same(d, com.legend.values.PureDateLiteral.parse("2014-01-01T00:00:00")));
        // the string-carrier bridge DIED with the cutover (partial
        // precision rides the wire natively now): string vs temporal is
        // a TYPE mismatch, false like pure
        assertFalse(same("2014-01-01", d));
        assertFalse(same(d, "2014-01-01"));
    }

    // (toRepresentation's spellings, legend-pure's print through EdgePureWords.repr: gone with it, item 3's S2 --
    // legend-pure's printing at the compatibility edge is B2's, rule 3's closed list)

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
                new BigDecimal("1E-50")), "exact kinds must compare exactly");
        assertNull(tolMsg(e, a,
                new BigDecimal("1E-40")));
        // floating sides keep double arithmetic
        assertNull(tolMsg(1.0d, 1.0d + 1e-16, 1e-13));
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
                Arrays.asList(t1, d2), Arrays.asList(d2, t1), LITE));
    }

    @Test
    @DisplayName("P2-5: assertEq — primitives compare, non-primitives are LOUD")
    void assertEqSemantics() {
        assertNull(eqOneMsg(1L, 1L, LITE));
        assertNotNull(eqOneMsg(1L, 2L, LITE));
        assertThrows(com.legend.error.NotImplementedException.class,
                () -> eqOneMsg(Map.of("a", 1L), Map.of("a", 1L), LITE),
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
