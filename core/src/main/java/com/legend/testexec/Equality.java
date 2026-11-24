// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;



import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HOST MODE — the one judge (docs/JUDGING_TWO_MODES_2026_09_17.md §2, step 2).
 *
 * <p>One class decides every equality the host answers. It receives values
 * PAIRED WITH THEIR DECLARED KIND ({@link Typed}) and answers the engine's
 * own rules, literally: same kind, then equal by value — Integer exact,
 * Decimal by the engine's scale-sensitive equals, Float exact with ONE declared leniency (two
 * units in the last place of the larger magnitude, Double × Double, finite,
 * COUNTED every time it fires), String / Boolean / dates exact; collections
 * ordered or as a multiset as the assertion says (the engine's
 * {@code assertSameElements} = sort, then ordered); class instances and maps
 * structurally through the same leaf; the two JSON comparators the engine
 * has, each named ({@link #pureJson} — the Pure JSON model, a number's kind is
 * part of its value; (the service-test EqualToJson comparator moved to the edge, edge.EqualToJson, 2026-11-17) — the service-test
 * {@code JsonNodeComparator}, numbers by decimal value). Every entry returns
 * {@code null} for equal or the FIRST-DIFFERENCE narrative — never a boolean
 * a caller re-interprets.
 *
 * <p>Host mode never runs SQL to judge, never converts a value, never
 * consults a wire type. A declared kind of {@code Number} (or none) takes the
 * carrier's runtime kind, as the engine's runtime does. A shape it cannot
 * compare is {@link Unjudged}: the assert FAILS naming the shape.
 */
public final class Equality {

    private Equality() {
    }

    /** The kinds a value is compared under: its carrier's at run, or the one its declaration names where that is
     *  numeric (the plan-as-data design's step 1, 2026-11-08: the judge's own kinds, read off the value and the
     *  plan's type name -- until then the compiler's primitive types). */
    public enum Kind { INTEGER, FLOAT, DECIMAL, STRING, BOOLEAN, STRICT_DATE, DATE, DATE_TIME }

    /** The numeric kind a declared type names, in the plan's spelling ({@code Integer}, {@code Float},
     *  {@code Decimal}, a precise decimal {@code Decimal(p,s)}; a precise primitive spells as its base), or null: every
     *  other declaration keeps the carrier's kind ({@link #effectiveKind}). */
    static @com.legend.base.Nullable Kind declaredNumeric(@com.legend.base.Nullable String declared) {
        if (declared == null) {
            return null;
        }
        return switch (declared) {
            case "Integer" -> Kind.INTEGER;
            case "Float" -> Kind.FLOAT;
            case "Decimal" -> Kind.DECIMAL;
            default -> declared.startsWith("Decimal(") ? Kind.DECIMAL : null;
        };
    }

    /** A value with the type its declaration gave it, in the plan's spelling ({@code null} = take the carrier's
     *  kind). */
    public record Typed(@com.legend.base.Nullable Object value, @com.legend.base.Nullable String kind) {
        public static Typed of(@com.legend.base.Nullable Object value) {
            return new Typed(value, null);
        }

        public static List<Typed> all(List<Object> values, @com.legend.base.Nullable String kind) {
            List<Typed> out = new ArrayList<>(values.size());
            for (Object v : values) {
                out.add(new Typed(v, kind));
            }
            return out;
        }
    }

    /** A shape the host judge does not compare — the assert fails with it. */
    public static final class Unjudged extends RuntimeException {
        public Unjudged(String shape) {
            super("unjudged in host mode: " + shape);
        }
    }

    // ---- SCALARS ----------------------------------------------------------

    /** The kind a carrier has at runtime (the engine's runtime kind). */
    static @com.legend.base.Nullable Kind carrierKind(@com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> null;
            case BigDecimal ignored -> Kind.DECIMAL;
            case Double ignored -> Kind.FLOAT;
            case Float ignored -> Kind.FLOAT;
            case Long ignored -> Kind.INTEGER;
            case Integer ignored -> Kind.INTEGER;
            case Short ignored -> Kind.INTEGER;
            case Byte ignored -> Kind.INTEGER;
            case BigInteger ignored -> Kind.INTEGER;
            case String ignored -> Kind.STRING;
            case Boolean ignored -> Kind.BOOLEAN;
            case com.legend.values.PureDateLiteral.StrictDate ignored -> Kind.STRICT_DATE;
            case com.legend.values.PureDateLiteral ignored -> Kind.DATE;
            // JDBC temporal carriers (a date literal read back): the same
            // kind on both sides, equal by the carrier's own equality
            case java.time.LocalDate ignored -> Kind.STRICT_DATE;
            case java.util.Date ignored -> Kind.DATE;   // the driver's date and timestamp carriers
            case java.time.LocalDateTime ignored -> Kind.DATE_TIME;
            case java.time.OffsetDateTime ignored -> Kind.DATE_TIME;
            case java.time.Instant ignored -> Kind.DATE_TIME;
            default -> null;   // instances, collections
        };
    }

    /** The kind that decides — the engine's own boundary rule
     * ({@code dataTypeTransformer}, core_relational execution_relational_execute
     * .pure:299-320): a NUMERIC declaration converts the wire's cell (Float /
     * Number {@code * 1.0}, Decimal {@code toDecimal}); dates and Booleans
     * arrive typed from the database; EVERY OTHER declaration — String, Number
     * left unrefined, a class, none — is the identity {@code {a | $a}}: the
     * cell keeps the wire's kind. So {@code Account.number : String[1]} mapped
     * to {@code accountTable.id INT} (mapping::tree) delivers the Integer 11 in
     * the engine and in lite alike, and its test asserts {@code [11, 'OrgName3']}
     * against it — a declaration is not a cast. */
    private static @com.legend.base.Nullable Kind effectiveKind(Typed t) {
        Kind k = declaredNumeric(t.kind());
        Kind carrier = carrierKind(t.value());
        if ((k == Kind.FLOAT || k == Kind.DECIMAL
                || k == Kind.INTEGER) && isNumericKind(carrier)) {
            return k;
        }
        return carrier;
    }

    private static boolean isNumericKind(@com.legend.base.Nullable Kind k) {
        return k == Kind.FLOAT || k == Kind.DECIMAL
                || k == Kind.INTEGER;
    }

    /** The decision itself (the narrative is the caller's when it has a
     * better one — a row, a path). */
    public static boolean same(Typed et, Typed at, HostRules rules) {
        Object e = et.value();
        Object a = at.value();
        // a Variant is never the same as a String, a number or a Boolean of the same text (Pure's classifier is part of
        // the value; the audit of c2217801, 2026-11-23); two Variants are refused at planning (register S401, ruled by
        // the user 2026-11-23: legend-pure's identity), so two Variant values never meet here, and one equals nothing
        // (the narrow review of f9e74256: equal JSON had been called equal); an enumeration's value is carried as its
        // enumeration and name (LiteWords.EnumName, register S169), equal to the same enumeration's same name alone
        if (e == null || a == null || e instanceof LiteWords.Kinded || a instanceof LiteWords.Kinded) {
            return e == null && a == null || e instanceof LiteWords.EnumName && e.equals(a);
        }
        if ((e instanceof Map<?, ?> || e instanceof List<?>)
                && (a instanceof Map<?, ?> || a instanceof List<?>)) {
            // an object is never an array (the audit of c2217801: [] against {} recursed through the leaf without end)
            return (e instanceof Map<?, ?>) == (a instanceof Map<?, ?>) && structural(e, a, rules);
        }
        Kind ek = effectiveKind(et);
        Kind ak = effectiveKind(at);
        if (ek == null || ak == null) {
            if (e instanceof com.legend.values.PureDateLiteral
                    || a instanceof com.legend.values.PureDateLiteral) {
                return e.equals(a);
            }
            throw new Unjudged(e.getClass().getSimpleName() + " vs "
                    + a.getClass().getSimpleName());
        }
        if (ek != ak) {
            // Rule 3 (register S127, the user's ruling of 2026-11-04: "the principled fix for the product, then compiled
            // for backwards compatible"): a different kind is never equal in lite's own runs -- the type is part of the
            // value (1D == 1, 1 == 1.0 and 1.5 == 1.5D false, legend-pure's interpreted rule); in backwards-compatibility
            // mode compiled legend-pure's (CompiledSupport.eq(Number, Number), what a test's assert runs upstream): an
            // Integer and a Decimal equal by their texts (1D == 1), a Float equal to neither
            return rules.forms().equalAcrossNumberKinds(ek, ak, e, a);
        }
        if (ek == Kind.INTEGER) {
            if (!(isIntegral(e) && isIntegral(a))) {
                return false;   // an Integer-declared non-integral carrier
            }
            return toBigInteger(e).equals(toBigInteger(a));
        }
        if (ek == Kind.DECIMAL) {
            // the engine's assert seam: getValue().equals — SCALE-SENSITIVE
            // (VERDICT_RULE_AUDIT X2; the equality-worlds fixture pins
            // 3.0D ≠ 3.00D in World 1, while SQL '=' is scale-blind)
            if (!(e instanceof Number en && a instanceof Number an)) {
                return false;
            }
            // the run hook's form (train 17, 2026-11-18): lite's by value (register S139, the user's ruling of
            // 2026-11-05), as `==` does in the database; the edge's scale-sensitive, compiled legend-pure's
            boolean eq = rules.forms().equalDecimals(decimal(en), decimal(an));
            if (!eq && decimal(en).compareTo(decimal(an)) == 0) {
                // leg 3.0 census: the pair differs by SCALE only — the
                // witness set for the canon spec's Decimal amendment
                CanonicalDivergence.decimalScaleOnly();
            }
            return eq;
        }
        if (ek == Kind.FLOAT) {
            if (!(e instanceof Number en && a instanceof Number an)) {
                return false;
            }
            if (nonFinite(e) || nonFinite(a)) {
                return e instanceof Double de && a instanceof Double da
                        ? de.doubleValue() == da.doubleValue() : e.equals(a);
            }
            if (decimal(en).compareTo(decimal(an)) == 0) {
                return true;   // the carriers agree exactly (digits kept)
            }
            // two Floats whose values differ: the run hook's form (train 17, 2026-11-18) -- lite's exact (S15/S132, the
            // user's ruling of 2026-11-05: "I like our clean float"), the edge's compiled legend-pure 2 ULP
            return e instanceof Double de && a instanceof Double da && rules.forms().equalFloatsApart(de, da);
        }
        if (ek == Kind.STRING) {
            // two Strings compare exactly, as legend-pure's equal does, under every rule set and both judges (2026-11-06,
            // the user's "okay" to the S132 scope fix): S132's 2-ULP floats apply to a RENDERED result's text alone --
            // the host's rendered arm compares its cells (the edge's UpstreamRenderedTexts), the database judge its rows in the
            // same verdict row (VerdictSql.either) -- never to two Strings a body computed
            return e.equals(a);
        }
        if (ek == Kind.BOOLEAN) {
            return e.equals(a);
        }
        return rules.forms().equalDates(e, a);   // dates: the run hook's form (register S399)
    }

    private static boolean structural(Object e, Object a, HostRules rules) {
        return firstDiff(e, a, "$", (x, y) -> same(Typed.of(x), Typed.of(y), rules)) == null;
    }

    // ---- COLLECTIONS ------------------------------------------------------

    /** Element by element, in order: the decision alone (C5's step C, C-3, 2026-11-17: the first difference's text
     *  went, no caller read it; a message is the run's words, {@code PureWords} or lite's own). */
    public static boolean ordered(List<Typed> e, List<Typed> a, HostRules rules) {
        if (e.size() != a.size()) {
            return false;
        }
        for (int i = 0; i < e.size(); i++) {
            if (!same(e.get(i), a.get(i), rules)) {
                return false;
            }
        }
        return true;
    }

    /** The engine's {@code assertSameElements}: sort both sides by the value order, then ordered -- legend-pure's
     *  {@code assertEquals($expected->sort(), $actual->sort())} (assertSameElements.pure:11). An instance or a map has
     *  no value order: those are matched as a multiset, each expected one to an equal actual one, never lined up by
     *  their text (the instances design's HIGH, 2026-11-22: two instances whose fields are one value written two ways,
     *  {@code 1.5D} and {@code 1.50D}, sorted apart by their text and failed a pair the database judge holds). */
    public static boolean sameElements(List<Typed> e, List<Typed> a, HostRules rules) {
        List<Typed> es = sorted(e), as = sorted(a);
        int k = (int) es.stream().takeWhile(t -> !(t.value() instanceof Map<?, ?>)).count();
        return e.size() == a.size() && k == as.stream().takeWhile(t -> !(t.value() instanceof Map<?, ?>)).count()
                && ordered(es.subList(0, k), as.subList(0, k), rules)
                && rowMultiset(es.subList(k, es.size()), as.subList(k, as.size()), 1, rules);
    }

    /** A grid's flat cells paired with their COLUMN kinds (cycling per
     * row) — the TDS's declared column types decide, cell by cell. */
    public static List<Typed> grid(List<Object> cells, List<String> columnKinds) {
        int w = columnKinds.size();
        List<Typed> out = new ArrayList<>(cells.size());
        for (int i = 0; i < cells.size(); i++) {
            out.add(new Typed(cells.get(i), w == 0 ? null : columnKinds.get(i % w)));
        }
        return out;
    }

    /** Rows of width {@code w} as a MULTISET (each expected row consumes one
     * equal actual row); cells through {@link #same}. */
    public static boolean rowMultiset(List<Typed> e, List<Typed> a, int w, HostRules rules) {
        if (e.size() != a.size()) {
            return false;
        }
        List<List<Typed>> pool = new ArrayList<>(chunk(a, w));
        for (List<Typed> row : chunk(e, w)) {
            int hit = -1;
            for (int i = 0; i < pool.size() && hit < 0; i++) {
                if (ordered(row, pool.get(i), rules)) {
                    hit = i;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        return true;
    }

    private static List<List<Typed>> chunk(List<Typed> flat, int w) {
        List<List<Typed>> rows = new ArrayList<>();
        for (int i = 0; i + w <= flat.size(); i += w) {
            rows.add(flat.subList(i, i + w));
        }
        return rows;
    }

    static List<Typed> sorted(List<Typed> values) {
        List<Typed> out = new ArrayList<>(values);
        out.sort(java.util.Comparator.comparingInt((Typed t) -> typeRank(t.value()))
                .thenComparing(t -> withinRank(t.value())));
        return out;
    }

    private static int typeRank(@com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> 0;
            case Number n -> 1;
            case String s -> 2;
            case Boolean b -> 3;
            case com.legend.values.PureDateLiteral d -> 4;
            case Map<?, ?> m -> 5;
            case LiteWords.Kinded k -> 6;   // a Variant's JSON, an enumeration's value (S401, S169)
            default -> throw new Unjudged("sort over " + v.getClass().getName());
        };
    }

    @SuppressWarnings("unchecked")
    private static Comparable<Object> withinRank(@com.legend.base.Nullable Object v) {
        return (Comparable<Object>) (Comparable<?>) switch (v) {
            case null -> "";
            case Number n -> new BigDecimal(String.valueOf(n));
            case String s -> s;
            case Boolean b -> b;
            case com.legend.values.PureDateLiteral d -> d.toInstantFloor();
            case LiteWords.Kinded k -> k.sortKey();   // equal values sort together (LiteWords.Kinded)
            default -> "";   // instances and maps: no order of their own (sameElements matches them)
        };
    }

    // ---- JSON: the Pure JSON model (assertJsonStringsEqual) ---------------

    /** The Pure JSON model's equality: structure, keys, and leaves — a
     * JSONNumber's kind is part of its value ({@code 68} ≠ {@code 68.0}),
     * two decimals compare numerically. */
    public static @com.legend.base.Nullable String pureJson(@com.legend.base.Nullable Object expected,
            @com.legend.base.Nullable Object actual) {
        return firstDiff(expected, actual, "$", Equality::pureJsonLeaf);
    }

    /** {@link #pureJson} with the ROOT array as a multiset (a result that
     * carries no order). */
    public static @com.legend.base.Nullable String pureJsonUnorderedRoot(
            @com.legend.base.Nullable Object expected, @com.legend.base.Nullable Object actual) {
        if (expected instanceof List<?> el && actual instanceof List<?> al) {
            List<Object> unmatched = new ArrayList<>(al);
            List<Object> missing = new ArrayList<>();
            for (Object e : el) {
                int at = -1;
                for (int i = 0; i < unmatched.size() && at < 0; i++) {
                    if (firstDiff(e, unmatched.get(i), "$", Equality::pureJsonLeaf) == null) {
                        at = i;
                    }
                }
                if (at < 0) {
                    missing.add(e);
                } else {
                    unmatched.remove(at);
                }
            }
            if (missing.isEmpty() && unmatched.isEmpty()) {
                return null;
            }
            return "$ (root array as a multiset) expected " + el.size()
                    + " element(s), got " + al.size() + "; missing "
                    + abbreviate(canonicalText(missing)) + ", unexpected "
                    + abbreviate(canonicalText(unmatched));
        }
        return pureJson(expected, actual);
    }

    private static boolean pureJsonLeaf(@com.legend.base.Nullable Object e, @com.legend.base.Nullable Object a) {
        if (e instanceof BigDecimal be && a instanceof BigDecimal ba) {
            return be.compareTo(ba) == 0;
        }
        return java.util.Objects.equals(e, a);
    }

    static @com.legend.base.Nullable String firstDiff(@com.legend.base.Nullable Object e,
            @com.legend.base.Nullable Object a, String path,
            java.util.function.BiPredicate<Object, Object> leaf) {
        if (e instanceof Map<?, ?> em && a instanceof Map<?, ?> am) {
            if (!em.keySet().equals(am.keySet())) {
                return path + " expected keys " + em.keySet() + ", got " + am.keySet();
            }
            for (Object k : em.keySet()) {
                String d = firstDiff(em.get(k), am.get(k), path + "." + k, leaf);
                if (d != null) {
                    return d;
                }
            }
            return null;
        }
        if (e instanceof List<?> el && a instanceof List<?> al) {
            if (el.size() != al.size()) {
                return path + " expected " + el.size() + " element(s), got " + al.size();
            }
            for (int i = 0; i < el.size(); i++) {
                String d = firstDiff(el.get(i), al.get(i), path + "[" + i + "]", leaf);
                if (d != null) {
                    return d;
                }
            }
            return null;
        }
        return leaf.test(e, a) ? null
                : path + " expected " + abbreviate(String.valueOf(e))
                        + ", got " + abbreviate(String.valueOf(a));
    }

    static String canonicalText(@com.legend.base.Nullable Object v) {
        if (v instanceof Map<?, ?> m) {
            StringBuilder sb = new StringBuilder("{");
            m.keySet().stream().map(String::valueOf).sorted().forEach(k ->
                    sb.append(k).append(':').append(canonicalText(m.get(k))).append(','));
            return sb.append('}').toString();
        }
        if (v instanceof List<?> l) {
            StringBuilder sb = new StringBuilder("[");
            l.forEach(e -> sb.append(canonicalText(e)).append(','));
            return sb.append(']').toString();
        }
        if (v instanceof BigDecimal d) {
            return d.stripTrailingZeros().toPlainString();
        }
        return String.valueOf(v);
    }

    private static String abbreviate(String s) {
        return s.length() <= 120 ? s : s.substring(0, 120) + "…";
    }

    // ---- carriers -----------------------------------------------------------

    private static BigDecimal decimal(Number n) {
        return n instanceof BigDecimal bd ? bd : new BigDecimal(n.toString());
    }

    private static boolean nonFinite(Object v) {
        return (v instanceof Double d && !Double.isFinite(d))
                || (v instanceof Float f && !Float.isFinite(f));
    }

    private static BigInteger toBigInteger(Object v) {
        return v instanceof BigInteger bi ? bi : BigInteger.valueOf(((Number) v).longValue());
    }

    static boolean isIntegral(Object v) {
        return v instanceof Long || v instanceof Integer || v instanceof Short
                || v instanceof Byte || v instanceof BigInteger;
    }
}
