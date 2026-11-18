// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import static com.legend.testexecutionplan.Verdicts.wireId;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * THE PLATFORM ASSERT FAMILY'S DECISIONS (the platform's natives: assertEquals, assertSameElements, assertEq,
 * assertInstanceOf, assertEqWithinTolerance -- lite's own, their semantics verified against legend-pure's assert
 * files, never loaded from them): each decision is {@link Equality}'s under the run's {@link HostRules} (the plan's
 * data), and this class writes no word. A failure's text is asked of the {@link PureWords} hook, which the
 * compatibility edge alone implements (C5's step B, 2026-11-17); lite's own rules word through {@code LiteWords} and
 * never reach the message methods here. Mapped: census J17 (2026-11-18), CompatSpellingGuardTest.WORDED_BY_HOOK --
 * its only string literal is its refusal's.
 */
public final class PureAsserts {

    private PureAsserts() {
    }

    // ================================================================
    // The assert family (spec semantics; null = holds, else the spec's
    // failure message)
    // ================================================================

    /** The message for typed sides — the verdict's own cells, so the
     * narrative can never disagree with the decision. */
    public static @com.legend.base.Nullable String assertEqualsTyped(
            List<Equality.Typed> expected, List<Equality.Typed> actual, HostRules rules,
            PureWords words) {
        if (Equality.ordered(expected, actual, rules)) {
            return null;
        }
        List<Object> e = new ArrayList<>();
        expected.forEach(t -> e.add(t.value()));
        List<Object> a = new ArrayList<>();
        actual.forEach(t -> a.add(t.value()));
        return words.equals(e, a, null, null);
    }

    /** {@code assertSameElements(expected, actual)}
     * (assertSameElements.pure:17): {@code assertEquals(e->sort(),
     * a->sort())} — the multiset rule IS sort-then-ordered-equal. */
    public static @com.legend.base.Nullable String assertSameElements(
            List<Object> expected, List<Object> actual, HostRules rules,
            PureWords words) {
        return assertSameElements(expected, actual, null, null, rules, words);
    }

    /** With each side's STATIC type, an instance's {@code instanceOf} half (2026-11-05, the assert bar: two lists of
     *  pairs printed {@code instanceOf ?}). */
    public static @com.legend.base.Nullable String assertSameElements(List<Object> expected, List<Object> actual,
            @com.legend.base.Nullable String expectedClass,
            @com.legend.base.Nullable String actualClass,
            HostRules rules, PureWords words) {
        if (equal(sorted(expected), sorted(actual), rules)) {
            return null;
        }
        return sameElementsMessage(expected, actual, expectedClass, actualClass, words);
    }

    /** Upstream's assertSameElements text over two sides a verdict has already failed: both sorted, as lists of
     *  representations. It compares nothing (the ledger audit's D4, 2026-11-16: the message is the failed verdict's,
     *  never a second comparison that could hold where the typed one failed). */
    public static String sameElementsMessage(List<Object> expected, List<Object> actual,
            @com.legend.base.Nullable String expectedClass, @com.legend.base.Nullable String actualClass,
            PureWords words) {
        return words.sameElements(sorted(expected), sorted(actual), expectedClass, actualClass);
    }

    /** assertEqWithinTolerance's {@code abs(e - a) <= abs(delta)}, the decision alone: EXACT kinds (Integer, Decimal)
     *  in exact arithmetic (P2-1, 2026-08-19: the double round-trip widened the tolerance for high-precision
     *  Decimals); a floating side in double arithmetic, its values carrying no more precision than that. */
    public static boolean withinTolerance(Number expected, Number actual, Number delta) {
        boolean held;
        if (isExact(expected) && isExact(actual) && isExact(delta)) {
            held = toExact(expected).subtract(toExact(actual)).abs()
                    .compareTo(toExact(delta).abs()) <= 0;
        } else {
            held = Math.abs(expected.doubleValue() - actual.doubleValue())
                    <= Math.abs(delta.doubleValue());
        }
        return held;
    }

    private static boolean isExact(Number n) {
        return n instanceof BigDecimal || isIntegral(n);
    }

    private static BigDecimal toExact(Number n) {
        return n instanceof BigDecimal d ? d
                : BigDecimal.valueOf(n.longValue());
    }

    /** {@code assertEq(expected:Any[1], actual:Any[1])}
     * (assertEq.pure:17): {@code assert(eq(e, a), msg)} — {@code eq} is
     * IDENTITY-or-primitive equality (eq.pure doc). Primitives coincide
     * with {@code equal} (the spec's own {@code eq(6, 3+3)}); for
     * NON-primitives {@code eq} means SAME INSTANCE, which a value wire
     * cannot observe — LOUD, never a quiet structural answer (P2-5,
     * 2026-08-19 deep audit: the silent conflation risked answering
     * true where pure answers false). */
    public static @com.legend.base.Nullable String assertEq(
            @com.legend.base.Nullable Object expected,
            @com.legend.base.Nullable Object actual, HostRules rules,
            PureWords words) {
        return eqHolds(expected, actual, rules) ? null
                : words.pair(expected, wireType(expected), actual, wireType(actual));
    }

    /** {@code eq(e, a)}: the decision alone (identity for instances that carry one, loud for those that do not). */
    public static boolean eqHolds(@com.legend.base.Nullable Object expected, @com.legend.base.Nullable Object actual,
            HostRules rules) {
        if (isNonPrimitive(expected) || isNonPrimitive(actual)) {
            // F13 — identity IS observable when both wires carry the
            // synthetic __id (keyless-class instance maps, minted per
            // construction site): eq() is exactly id equality — ids are
            // unique per site, so cross-class values can never collide.
            String ei = wireId(expected);
            String ai = wireId(actual);
            if (ei != null && ai != null) {
                // upstream's own words (assertEq.pure: '\nexpected: %r\nactual:   %r'), each instance as %r writes it
                // (<id instanceOf Class>) -- the database judge's text (2026-11-04, the core parity program: "expected
                // and actual are distinct instances" was lite's)
                return ei.equals(ai);
            }
            throw new com.legend.error.NotImplementedException(
                    "assertEq over non-primitive values: eq is INSTANCE"
                    + " identity, which is not observable on a value wire"
                    + " (assertEquals is the structural compare)");
        }
        return Equality.same(Equality.Typed.of(expected), Equality.Typed.of(actual), rules);
    }

    private static boolean isNonPrimitive(@com.legend.base.Nullable Object v) {
        return v != null && !(v instanceof Number || v instanceof String
                || v instanceof Boolean || isTemporal(v));
    }

    /** The synthetic site identity a keyless-instance wire map carries
     * ({@code __id}, F13), or null when the value has no observable
     * identity. */
    private static @com.legend.base.Nullable String wireType(@com.legend.base.Nullable Object v) {
        return v instanceof java.util.Map<?, ?> m
                && m.get(com.legend.sql.SqlExpr.SYNTHETIC_TYPE) instanceof String t ? t : null;
    }

    /** {@code assertInstanceOf(instance, type)} (assertInstanceOf.pure:
     * {@code assert($instance->instanceOf($type), msg)}): the RUNTIME
     * kind of the database-produced carrier against the named type, up
     * the m3 value lattice (Integer/Float/Decimal {@code <:} Number;
     * temporals {@code <:} Date; everything {@code <:} Any). Null =
     * pass; a failure speaks the spec body's format (elementToPath of a
     * top-level primitive is its name). */
    public static @com.legend.base.Nullable String assertInstanceOf(
            @com.legend.base.Nullable Object v, String rawType, PureWords words) {
        String actual = notAnInstanceOf(v, rawType);
        return actual == null ? null : words.notInstanceOf(v, null, bare(rawType), actual);
    }

    /** The m3 primitive path ({@code meta::pure::metamodel::type::Integer}) as its bare spelling, which names the same
     *  type. */
    private static String bare(String rawType) {
        return rawType.substring(rawType.lastIndexOf(':') + 1);
    }

    /** {@code v}'s carrier's type where {@code v} is not a {@code rawType}, else null: the decision alone. */
    public static @com.legend.base.Nullable String notAnInstanceOf(@com.legend.base.Nullable Object v, String rawType) {
        String type = bare(rawType);
        String actual = carrierTypeName(v);
        boolean ok = switch (type) {
            case "Any", com.legend.result.RowDecode.ANY -> true;
            case "Number" -> actual.equals("Integer")
                    || actual.equals("Float") || actual.equals("Decimal");
            case "Date" -> actual.equals("StrictDate")
                    || actual.equals("DateTime") || actual.equals("Date");
            default -> actual.equals(type);
        };
        return ok ? null : actual;
    }

    private static String carrierTypeName(@com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> "Nil";
            case Byte ignored -> "Integer";
            case Short ignored -> "Integer";
            case Integer ignored -> "Integer";
            case Long ignored -> "Integer";
            case java.math.BigInteger ignored -> "Integer";
            case Float ignored -> "Float";
            case Double ignored -> "Float";
            case java.math.BigDecimal ignored -> "Decimal";
            case Boolean ignored -> "Boolean";
            case String ignored -> "String";
            case com.legend.values.PureDateLiteral.Year ignored -> "Date";
            case com.legend.values.PureDateLiteral.YearMonth ignored -> "Date";
            case com.legend.values.PureDateLiteral.StrictDate ignored -> "StrictDate";
            case com.legend.values.PureDateLiteral d -> "DateTime";
            case java.time.OffsetDateTime ignored -> "DateTime";
            default -> v.getClass().getSimpleName();
        };
    }

    // ================================================================
    // equal() — pure's equality over wire values (ONE owner)
    // ================================================================

    /** Pure {@code equal(left:Any[*], right:Any[*])} (equal.pure):
     * collection equality is ordered, element-wise. */
    // ---- messages only: the decision is Equality's (host mode's one judge)

    /** {@code equal(e, a)}: the decision alone. */
    public static boolean holds(List<Object> left, List<Object> right,
            HostRules rules) {
        return equal(left, right, rules);
    }

    private static boolean equal(List<Object> left, List<Object> right, HostRules rules) {
        return Equality.ordered(Equality.Typed.all(left, null),
                Equality.Typed.all(right, null), rules);
    }

    private static boolean isIntegral(Object v) {
        return Equality.isIntegral(v);
    }

    private static boolean isTemporal(Object v) {
        return v instanceof com.legend.values.PureDateLiteral;
    }

    // ================================================================
    // sort() — pure's total order over Any, for assertSameElements
    // (numbers by value, then strings lexically, then booleans,
    // temporals by instant — the spec's own failure examples pin
    // number-before-string: sort([1, 3, '2']) = [1, 3, '2'])
    // ================================================================

    static List<Object> sorted(List<Object> values) {
        List<Object> out = new ArrayList<>(values.size());
        for (Equality.Typed t : Equality.sorted(Equality.Typed.all(values, null))) {
            out.add(t.value());
        }
        return out;
    }
}
