// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

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
    private static boolean same(@com.legend.base.Nullable Object e, @com.legend.base.Nullable Object a) {
        return Equality.same(Equality.Typed.of(e), Equality.Typed.of(a));
    }

    private static boolean ordered(List<Object> e, List<Object> a) {
        return Equality.ordered(Equality.Typed.all(e, null), Equality.Typed.all(a, null)) == null;
    }


    // ---- assertEquals.pure's own tests --------------------------------

    @Test
    @DisplayName("spec: testSuccessAssertEquals + WithCollections")
    void specSuccess() {
        assertNull(PureAsserts.assertEquals(List.of(1L), List.of(1L)));
        assertNull(PureAsserts.assertEquals(List.of("aaa"), List.of("aaa")));
        assertNull(PureAsserts.assertEquals(List.of(1L, 2L), List.of(1L, 2L)));
        assertNull(PureAsserts.assertEquals(
                List.of("aaa", 2L), List.of("aaa", 2L)));
    }

    @Test
    @DisplayName("spec: testFailureAssertEquals — the exact message")
    void specFailureMessage() {
        assertEquals("\nexpected: 1\nactual:   2",
                PureAsserts.assertEquals(List.of(1L), List.of(2L)));
    }

    @Test
    @DisplayName("spec: testFailureAssertEqualsWithCollections — exact messages")
    void specFailureCollections() {
        assertEquals("\nexpected: [1, 3, 2]\nactual:   [2, 4, 1, 5]",
                PureAsserts.assertEquals(List.of(1L, 3L, 2L),
                        List.of(2L, 4L, 1L, 5L)));
        assertEquals("\nexpected: [1, 2]\nactual:   [2, 1]",
                PureAsserts.assertEquals(List.of(1L, 2L), List.of(2L, 1L)));
        assertEquals("\nexpected: ['aaa', 2]\nactual:   [2, 'aaa']",
                PureAsserts.assertEquals(List.of("aaa", 2L),
                        List.of(2L, "aaa")));
    }

    // ---- assertSameElements.pure's own tests --------------------------

    @Test
    @DisplayName("spec: testSuccessAssertSameElements")
    void sameElementsSuccess() {
        assertNull(PureAsserts.assertSameElements(
                List.of(1L, 2L), List.of(2L, 1L)));
        assertNull(PureAsserts.assertSameElements(
                List.of("aaa", 2L), List.of(2L, "aaa")));
    }

    @Test
    @DisplayName("spec: testFailureAssertSameElements — SORTED renders, number-before-string")
    void sameElementsFailureMessage() {
        assertEquals("\nexpected: [1, 2, 3]\nactual:   [1, 2, 4, 5]",
                PureAsserts.assertSameElements(List.of(1L, 3L, 2L),
                        List.of(2L, 4L, 1L, 5L)));
        assertEquals("\nexpected: [1, 3, '2']\nactual:   [1, 4, 5, '2']",
                PureAsserts.assertSameElements(List.of(1L, 3L, "2"),
                        List.of("2", 4L, 1L, 5L)));
    }

    // ---- assertSize.pure ----------------------------------------------

    @Test
    @DisplayName("spec: assertSize message form")
    void sizeMessage() {
        assertNull(PureAsserts.assertSize(List.of(1L, 2L), 2));
        assertEquals("expected size: 3, actual size: 2",
                PureAsserts.assertSize(List.of(1L, 2L), 3));
    }

    // ---- the adjudicated wire policies (documented divergences) -------

    @Test
    @DisplayName("policy: TDSNull sentinel is expected-direction ONLY")
    void tdsNullSentinel() {
        assertTrue(same("TDSNull", null));
        assertFalse(same(null, "TDSNull"),
                "a literal 'TDSNull' on OUR wire where a NULL belongs"
                        + " must FAIL (audit 16 F5)");
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
    @DisplayName("policy: Decimal scale-SENSITIVE (the engine assert seam); 2-ULP doubles only")
    void numericPolicies() {
        // X2 (VERDICT_RULE_AUDIT): engine Decimal equality is
        // getValue().equals — SCALE-SENSITIVE
        assertFalse(same(
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
        assertEquals("'a\\'b'", PureAsserts.repr("a'b"));
        assertEquals("'a\\nb'", PureAsserts.repr("a\nb"));
        assertEquals("%2014-01-01", PureAsserts.repr(
                com.legend.values.PureDateLiteral.parse("2014-01-01")));
        // partial precision reprs exactly as written — impossible on
        // the old java.time carrier
        assertEquals("%2014-01", PureAsserts.repr(
                com.legend.values.PureDateLiteral.parse("2014-01")));
        assertEquals("3.14D", PureAsserts.repr(new BigDecimal("3.14")));
        assertEquals("1", PureAsserts.repr(1L));
        assertEquals("true", PureAsserts.repr(Boolean.TRUE));
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
        assertNotNull(PureAsserts.assertEqWithinTolerance(e, a,
                new BigDecimal("1E-50")), "exact kinds must compare exactly");
        assertNull(PureAsserts.assertEqWithinTolerance(e, a,
                new BigDecimal("1E-40")));
        // floating sides keep double arithmetic
        assertNull(PureAsserts.assertEqWithinTolerance(1.0d, 1.0d + 1e-16, 1e-13));
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
        assertNull(PureAsserts.assertSameElements(
                Arrays.asList(t1, d2), Arrays.asList(d2, t1)));
    }

    @Test
    @DisplayName("P2-5: assertEq — primitives compare, non-primitives are LOUD")
    void assertEqSemantics() {
        assertNull(PureAsserts.assertEq(1L, 1L));
        assertNotNull(PureAsserts.assertEq(1L, 2L));
        assertThrows(com.legend.error.NotImplementedException.class,
                () -> PureAsserts.assertEq(Map.of("a", 1L), Map.of("a", 1L)),
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
