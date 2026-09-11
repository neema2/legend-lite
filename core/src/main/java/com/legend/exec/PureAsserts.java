// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * THE PLATFORM ASSERT FAMILY (One-Platform Plan Phase 2a, Clause 2b —
 * legend-pure's assert files are the SPEC, this Java is the platform's
 * definition): {@code assertEquals} = {@code assert(equal(e, a), msg)};
 * {@code assertSameElements(e, a)} = {@code assertEquals(sort(e),
 * sort(a))}; {@code assertSize(c, n)} = {@code assertEq(n,
 * c->size())} — read from
 * {@code platform/pure/essential/tests/*.pure} verbatim. Failure
 * messages use the spec's own {@code toRepresentation} +
 * {@code joinStrings} forms.
 *
 * <p>TWO layers, deliberately separated in this one owner:
 * <ol>
 * <li><b>SPEC CORE</b> — pure {@code equal()} over wire values
 * (ordered collection equality; integral kinds normalize, Decimal by
 * {@code compareTo}: pure numeric equality is by VALUE within kind),
 * {@code toRepresentation}, the message formats.</li>
 * <li><b>ADJUDICATED WIRE POLICIES</b> (moved from the harness's
 * {@code wireEquals} at its deletion — the policies survive, the
 * private copy dies): the {@code TDSNull} sentinel (an expected
 * {@code 'TDSNull'} equals an actual NULL cell — never a genuine
 * payload); the 2-ULP dialect-arithmetic leniency (corpus float
 * expectations encode H2's libm; DuckDB differs in the last ULP —
 * DOUBLE-vs-DOUBLE only, exact kinds stay strict); the temporal
 * Any-carrier bridge (a mixed-collection literal's date decodes as its
 * JSON string — parse-and-compare, EXPECTED-string direction only: an
 * actual-side string where the engine returns a Date is a typing bug
 * this compare must catch, never bridge).</li>
 * </ol>
 */
public final class PureAsserts {

    private PureAsserts() {
    }

    // ================================================================
    // The assert family (spec semantics; null = holds, else the spec's
    // failure message)
    // ================================================================

    /** {@code assertEquals(expected:Any[*], actual:Any[*])}
     * (assertEquals.pure:17): {@code assert(equal(e, a), msg)} with the
     * spec's own message — {@code %r} single values, represented-and-
     * joined collections. */
    public static @com.legend.Nullable String assertEquals(
            List<Object> expected, List<Object> actual) {
        return assertEquals(expected, actual, null, null);
    }

    /** With each side's STATIC type (the {@code instanceOf Type} half of
     * an instance's representation — the wire map carries no type). */
    public static @com.legend.Nullable String assertEquals(
            List<Object> expected, List<Object> actual,
            com.legend.compiler.element.type.@com.legend.Nullable Type expectedType,
            com.legend.compiler.element.type.@com.legend.Nullable Type actualType) {
        if (equal(expected, actual)) {
            return null;
        }
        return "\nexpected: " + reprSide(expected, classFqnOf(expectedType))
                + "\nactual:   " + reprSide(actual, classFqnOf(actualType));
    }

    private static @com.legend.Nullable String classFqnOf(
            com.legend.compiler.element.type.@com.legend.Nullable Type t) {
        return t instanceof com.legend.compiler.element.type.Type.ClassType c
                ? c.fqn() : null;
    }

    /** {@code assertSameElements(expected, actual)}
     * (assertSameElements.pure:17): {@code assertEquals(e->sort(),
     * a->sort())} — the multiset rule IS sort-then-ordered-equal. */
    public static @com.legend.Nullable String assertSameElements(
            List<Object> expected, List<Object> actual) {
        List<Object> es = sorted(expected);
        List<Object> as = sorted(actual);
        if (equal(es, as)) {
            return null;
        }
        return "\nexpected: " + joined(es) + "\nactual:   " + joined(as);
    }

    /** {@code assertSize(collection, size)} (assertSize.pure:17). */
    public static @com.legend.Nullable String assertSize(
            List<Object> collection, long size) {
        if (collection.size() == size) {
            return null;
        }
        return "expected size: " + size + ", actual size: "
                + collection.size();
    }

    /** {@code assertEqWithinTolerance(expected, actual, delta)}
     * (assertEqWithinTolerance.pure): {@code abs(e - a) <= abs(delta)},
     * message in the spec's {@code %r} form. EXACT kinds (Integer,
     * Decimal) compare in EXACT arithmetic — the spec's subtraction is
     * pure number math (P2-1, 2026-08-19 deep audit: the double
     * round-trip silently widened the tolerance for high-precision
     * Decimals); a floating side keeps double arithmetic, its values
     * carry no more precision than that. */
    public static @com.legend.Nullable String assertEqWithinTolerance(
            Number expected, Number actual, Number delta) {
        boolean held;
        if (isExact(expected) && isExact(actual) && isExact(delta)) {
            held = toExact(expected).subtract(toExact(actual)).abs()
                    .compareTo(toExact(delta).abs()) <= 0;
        } else {
            held = Math.abs(expected.doubleValue() - actual.doubleValue())
                    <= Math.abs(delta.doubleValue());
        }
        if (held) {
            return null;
        }
        return "\nexpected: " + repr(expected)
                + "\nactual:   " + repr(actual);
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
    public static @com.legend.Nullable String assertEq(
            @com.legend.Nullable Object expected,
            @com.legend.Nullable Object actual) {
        if (isNonPrimitive(expected) || isNonPrimitive(actual)) {
            // F13 — identity IS observable when both wires carry the
            // synthetic __id (keyless-class instance maps, minted per
            // construction site): eq() is exactly id equality — ids are
            // unique per site, so cross-class values can never collide.
            String ei = wireId(expected);
            String ai = wireId(actual);
            if (ei != null && ai != null) {
                // repr() has no instance-map form (loud by design) —
                // the failure names the identities themselves
                return ei.equals(ai) ? null
                        : "\nexpected and actual are distinct instances"
                                + " (eq is identity)";
            }
            throw new com.legend.error.NotImplementedException(
                    "assertEq over non-primitive values: eq is INSTANCE"
                    + " identity, which is not observable on a value wire"
                    + " (assertEquals is the structural compare)");
        }
        if (equalScalar(expected, actual)) {
            return null;
        }
        return "\nexpected: " + repr(expected)
                + "\nactual:   " + repr(actual);
    }

    private static boolean isNonPrimitive(@com.legend.Nullable Object v) {
        return v != null && !(v instanceof Number || v instanceof String
                || v instanceof Boolean || isTemporal(v));
    }

    /** The synthetic site identity a keyless-instance wire map carries
     * ({@code __id}, F13), or null when the value has no observable
     * identity. */
    private static @com.legend.Nullable String wireId(
            @com.legend.Nullable Object v) {
        return v instanceof java.util.Map<?, ?> m
                && m.get(com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID)
                        instanceof String id ? id : null;
    }

    /** {@code assertInstanceOf(instance, type)} (assertInstanceOf.pure:
     * {@code assert($instance->instanceOf($type), msg)}): the RUNTIME
     * kind of the database-produced carrier against the named type, up
     * the m3 value lattice (Integer/Float/Decimal {@code <:} Number;
     * temporals {@code <:} Date; everything {@code <:} Any). Null =
     * pass; a failure speaks the spec body's format (elementToPath of a
     * top-level primitive is its name). */
    public static @com.legend.Nullable String assertInstanceOf(
            @com.legend.Nullable Object v, String rawType) {
        // the m3 primitive path (meta::pure::metamodel::type::Integer)
        // and the bare spelling name the same type — compare bare
        String type = rawType.substring(rawType.lastIndexOf(':') + 1);
        String actual = carrierTypeName(v);
        boolean ok = switch (type) {
            case "Any", com.legend.compiler.element.type.PlatformTypes.ANY -> true;
            case "Number" -> actual.equals("Integer")
                    || actual.equals("Float") || actual.equals("Decimal");
            case "Date" -> actual.equals("StrictDate")
                    || actual.equals("DateTime") || actual.equals("Date");
            default -> actual.equals(type);
        };
        return ok ? null : "expected " + repr(v) + " to be an instance of "
                + type + ", actual: " + actual;
    }

    private static String carrierTypeName(@com.legend.Nullable Object v) {
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
    public static boolean equal(List<Object> left, List<Object> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (!equalScalar(left.get(i), right.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** Element equality: the spec core plus the adjudicated wire
     * policies (class doc). The EXPECTED side is the corpus's literal;
     * the ACTUAL side is the platform's produced value — the sentinel
     * and the temporal bridge are direction-aware for exactly that
     * reason. */
    public static boolean equalScalar(@com.legend.Nullable Object e,
            @com.legend.Nullable Object a) {
        // POLICY: the TDSNull wire sentinel (expected-direction only —
        // a literal 'TDSNull' on OUR wire where a NULL belongs must
        // fail, the symmetric grant would mask it; audit 16 F5)
        if ("TDSNull".equals(e) && a == null) {
            return true;
        }
        if (e == null || a == null) {
            return e == a;
        }
        // X1-X4 (VERDICT_RULE_AUDIT, engine EqualityUtilities.eq):
        // primitive equality requires the SAME primitive kind — there
        // is NO cross-kind numeric equality in the engine (the old
        // integral×Decimal grant MIS-CITED its witness; Float×Decimal
        // and every other cross pair are FALSE). Same-kind rules:
        boolean eInt = isIntegral(e);
        boolean aInt = isIntegral(a);
        if (eInt || aInt) {
            if (!(eInt && aInt)) {
                return false;
            }
            // HUGEINT range: longValue() OVERFLOWS BigInteger carriers —
            // a genuine bug the deleted X1 cross-kind grant had been
            // masking (testLargePlus exposed it the moment the grant
            // died); integral equality widens to BigInteger when needed
            if (e instanceof BigInteger || a instanceof BigInteger) {
                return toBigInteger(e).equals(toBigInteger(a));
            }
            return ((Number) e).longValue() == ((Number) a).longValue();
        }
        if (e instanceof BigDecimal || a instanceof BigDecimal) {
            // X2: engine Decimal equality is getValue().equals —
            // SCALE-SENSITIVE (its tests spell the exact SQL-arithmetic
            // scale and pass strict in both engine lanes); a break here
            // is OUR scale drift, fixed at emission, never re-blurred
            return e instanceof BigDecimal be && a instanceof BigDecimal ba
                    && be.equals(ba);
        }
        boolean eFp = e instanceof Double || e instanceof Float;
        boolean aFp = a instanceof Double || a instanceof Float;
        if (eFp || aFp) {
            if (!(eFp && aFp)) {
                return false;
            }
            // NON-FINITE first: NaN never equals anything (IEEE; the
            // engine's BigDecimal-backed floats cannot even hold it);
            // infinities compare by identity
            if (nonFinite(e) || nonFinite(a)) {
                return e instanceof Double de2 && a instanceof Double da2
                        ? de2.doubleValue() == da2.doubleValue()
                        : e.equals(a);
            }
            // engine Float equality = equals over CANONICALIZED
            // BigDecimal (FloatCoreInstance.canonicalizeBigDecimal) —
            // for finite doubles the shortest-repr string compare IS
            // that canonical-form equality (and unifies zeros)
            if (new BigDecimal(String.valueOf(e))
                    .compareTo(new BigDecimal(String.valueOf(a))) == 0) {
                return true;
            }
            // POLICY: 2-ULP dialect-arithmetic leniency — DOUBLE vs
            // DOUBLE only; NaN, exact zero, and Decimal stay strict
            if (e instanceof Double de && a instanceof Double da
                    && !de.isNaN() && !da.isNaN()) {
                double ulp = Math.ulp(Math.max(Math.abs(de), Math.abs(da)));
                boolean ok = Math.abs(de - da) <= 2 * ulp;
                // audit 23 D2 measurement instrument (rides the policy)
                if (ok && de.doubleValue() != da.doubleValue()
                        && System.getenv("LL_TOL_COUNT") != null) {
                    System.err.println("[tol] ulp " + de + " vs " + da);
                }
                return ok;
            }
            return false;
        }
        // TEMPORALS: PureDateLiteral record equality IS the engine's
        // precision-sensitive PureDate.equals (their variants compare
        // every component INCLUDING precision) — the old string-carrier
        // bridge died with the D-arc cutover: partial-precision values
        // now ride the wire as PureDateLiteral (the fetch seam parses
        // the precision-faithful VARCHAR convention), so a string
        // beside a temporal is a TYPE mismatch, false like pure.
        // WIRE-VALUE TREES (struct cells decoded to maps at egress, and
        // any lists nested inside them): the ONE walker owns the
        // structure, THIS method stays the leaf rule (P2-4/P2-6,
        // 2026-08-19 deep audit — the private Map arm was undocumented
        // and nested lists fell through to raw Java equals with no pure
        // numeric semantics)
        if ((e instanceof Map<?, ?> && a instanceof Map<?, ?>)
                || (e instanceof List<?> && a instanceof List<?>)) {
            return JsonCompare.wireTree(e, a);
        }
        return e.equals(a);
    }

    private static boolean nonFinite(Object v) {
        return (v instanceof Double d && !Double.isFinite(d))
                || (v instanceof Float f && !Float.isFinite(f));
    }

    private static BigInteger toBigInteger(Object v) {
        return v instanceof BigInteger bi ? bi
                : BigInteger.valueOf(((Number) v).longValue());
    }

    private static boolean isIntegral(Object v) {
        return v instanceof Long || v instanceof Integer
                || v instanceof Short || v instanceof Byte
                || v instanceof BigInteger;
    }

    private static boolean isTemporal(Object v) {
        // THE wire temporal type ONLY (D-arc 2026-08-21): a java.sql or
        // java.time temporal reaching a compare is a fetch-seam LEAK —
        // it falls through to e.equals(a) (never true cross-kind) and
        // the canonical-divergence census reports it as unmodeled-kind
        return v instanceof com.legend.values.PureDateLiteral;
    }


    // ================================================================
    // toRepresentation() — pure source spelling of a value (ONE owner;
    // toRepresentation.pure — the testdatagen port, generalized)
    // ================================================================

    /** Pure {@code toRepresentation(any:Any[1])}: strings
     * backslash-escape, temporals take the {@code %} literal form,
     * Decimal the {@code D} suffix. A class INSTANCE (a wire map) takes
     * the spec's {@code <id instanceOf T>} form (toRepresentation.pure:
     * 28): the id is the synthetic site identity when the wire carries
     * one (F13), else the row's property values in wire order — the
     * identity a value wire can observe (identity as data); T is the
     * side's static class. A NAMED packageable element would render as
     * its path (toRepresentation.pure:27) — a wire map carries no path
     * (named gap, grows by witness). */
    public static String repr(@com.legend.Nullable Object v) {
        return repr(v, null);
    }

    public static String repr(@com.legend.Nullable Object v,
            @com.legend.Nullable String instanceClass) {
        if (v instanceof java.util.Map<?, ?> m) {
            String id = wireId(v);
            if (id == null) {
                StringBuilder sb = new StringBuilder();
                for (Object val : m.values()) {
                    if (sb.length() > 0) {
                        sb.append('|');
                    }
                    sb.append(val);
                }
                id = sb.toString();
            }
            return "<" + id + " instanceOf "
                    + (instanceClass == null ? "?" : instanceClass) + ">";
        }
        return switch (v) {
            case null -> "[]";   // an empty [0..1] renders as empty
            case String s -> "'" + s.replace("\\", "\\\\")
                    .replace("'", "\\'").replace("\n", "\\n") + "'";
            case BigDecimal d -> d.toPlainString() + "D";
            case Number n -> n.toString();
            case Boolean b -> b.toString();
            // THE wire temporal type (D-arc: sql/java.time never escape
            // the fetch) — %-prefixed engine spelling, UTC-normalized
            case com.legend.values.PureDateLiteral d ->
                    "%" + d.toEngineString();
            default -> throw new com.legend.error.NotImplementedException(
                    "toRepresentation for " + v.getClass().getName()
                    + " is not modeled (spec: '<id instanceOf Type>' —"
                    + " loud until a consumer pins the id form)");
        };
    }

    /** The spec's single-value message side ({@code %r}) or the
     * collection form ({@code joinStrings('[', ', ', ']')}). */
    private static String reprSide(List<Object> side) {
        return reprSide(side, null);
    }

    private static String reprSide(List<Object> side,
            @com.legend.Nullable String instanceClass) {
        if (side.size() == 1) {
            return repr(side.get(0), instanceClass);
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < side.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(repr(side.get(i), instanceClass));
        }
        return sb.append(']').toString();
    }

    private static String joined(List<Object> side) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < side.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(repr(side.get(i)));
        }
        return sb.append(']').toString();
    }

    // ================================================================
    // sort() — pure's total order over Any, for assertSameElements
    // (numbers by value, then strings lexically, then booleans,
    // temporals by instant — the spec's own failure examples pin
    // number-before-string: sort([1, 3, '2']) = [1, 3, '2'])
    // ================================================================

    static List<Object> sorted(List<Object> values) {
        List<Object> out = new ArrayList<>(values);
        out.sort(Comparator.comparingInt(PureAsserts::typeRank)
                .thenComparing(PureAsserts::withinRank));
        return out;
    }

    private static int typeRank(@com.legend.Nullable Object v) {
        // EXPLICIT kinds only (Charter C2.4: an unmatched kind THROWS,
        // never becomes a plausible bucket)
        return switch (v) {
            case null -> 0;
            case Number n -> 1;
            case String s -> 2;
            case Boolean b -> 3;
            case com.legend.values.PureDateLiteral d -> 4;
            case Map<?, ?> m -> 5;
            default -> throw new com.legend.error.NotImplementedException(
                    "assertSameElements sort over "
                            + v.getClass().getName() + " is not modeled");
        };
    }

    @SuppressWarnings("unchecked")
    private static Comparable<Object> withinRank(@com.legend.Nullable Object v) {
        return (Comparable<Object>) (Comparable<?>) switch (v) {
            case null -> "";
            case Number n -> new BigDecimal(String.valueOf(n));
            case String s -> s;
            case Boolean b -> b;
            // temporals BY INSTANT (P2-2, 2026-08-19 deep audit: the
            // section contract said instant, the code said text — a
            // date-only vs midnight-datetime mix text-sorted wrong;
            // the reference native compares temporals by components)
            case com.legend.values.PureDateLiteral d -> d.toInstantFloor();
            default -> String.valueOf(v);   // maps: stable text order
        };
    }
}
