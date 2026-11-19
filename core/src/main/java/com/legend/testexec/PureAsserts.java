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
 * data), and this class writes no word. A failure's text was asked of the PureWords hook, which the
 * compatibility edge alone implements (C5's step B, 2026-11-17). Since census J29 (2026-11-18) it holds the decisions
 * alone: every failure's words are the run's {@link HostWords} ({@link LiteHostWords}, or the edge's legend-pure words).
 * Mapped: census J17 (2026-11-18), CompatSpellingGuardTest.WORDED_BY_HOOK -- its only string literal is its
 * refusal's.
 */
public final class PureAsserts {

    private PureAsserts() {
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

    /** {@code eq(e, a)}: the decision alone (identity for instances that carry one, loud for those that do not); each
     *  primitive compared under its side's DECLARED kind where that is numeric ({@code eDeclared},
     *  {@code aDeclared}: the plan's type names, {@link Equality.Typed}), the carrier's otherwise -- as
     *  assertEquals' sides are (PR #118 fix round, 2026-11-18, G2: a Decimal property over an INTEGER column arrives as
     *  the column's integer, and the database judge compares it as the Decimal it is declared). */
    public static boolean eqHolds(@com.legend.base.Nullable Object expected, @com.legend.base.Nullable String eDeclared,
            @com.legend.base.Nullable Object actual, @com.legend.base.Nullable String aDeclared, HostRules rules) {
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
        return Equality.same(new Equality.Typed(expected, eDeclared), new Equality.Typed(actual, aDeclared), rules);
    }

    private static boolean isNonPrimitive(@com.legend.base.Nullable Object v) {
        return v != null && !(v instanceof Number || v instanceof String
                || v instanceof Boolean || isTemporal(v));
    }

    /** The m3 primitive path ({@code meta::pure::metamodel::type::Integer}) as its bare spelling, which names the same
     *  type. */
    public static String bare(String rawType) {
        return rawType.substring(rawType.lastIndexOf(':') + 1);
    }

    /** assertInstanceOf's decision: {@code v}'s carrier's type where {@code v} is not a {@code rawType}, else null --
     *  the RUNTIME kind of the database-produced carrier against the named type, up the m3 value lattice
     *  (Integer/Float/Decimal {@code <:} Number; temporals {@code <:} Date; everything {@code <:} Any). */
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

    public static List<Object> sorted(List<Object> values) {
        List<Object> out = new ArrayList<>(values.size());
        for (Equality.Typed t : Equality.sorted(Equality.Typed.all(values, null))) {
            out.add(t.value());
        }
        return out;
    }
}
