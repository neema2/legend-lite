// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.compiler.element.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
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
 * part of its value; {@link #serviceJson} — the service-test
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

    /** A value with the kind its declaration gave it ({@code null} kind =
     * take the carrier's). */
    public record Typed(@com.legend.base.Nullable Object value, @com.legend.base.Nullable Type kind) {
        public static Typed of(@com.legend.base.Nullable Object value) {
            return new Typed(value, null);
        }

        public static List<Typed> all(List<Object> values, @com.legend.base.Nullable Type kind) {
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

    // ---- THE LENIENCY (one, counted) --------------------------------------


    /** THE ONE FLOAT LENIENCY (§5a, one home): two finite doubles within
     * {@code 2 * ulp(max(|x|, |y|))}. The host cell rule (TdsCompare) and
     * the referee (H2Verify) call HERE; the database judge spells the same
     * predicate in SQL (VerdictSql.lenient). */
    public static boolean withinTwoUlp(double x, double y) {
        if (!Double.isFinite(x) || !Double.isFinite(y)) {
            return false;
        }
        double ulp = Math.ulp(Math.max(Math.abs(x), Math.abs(y)));
        boolean ok = Math.abs(x - y) <= 2 * ulp;
        if (ok && x != y) {
            Census.inc(Census.Key.ULP_FIRINGS);
            if (System.getenv("LL_TOL_COUNT") != null) {
                System.err.println("[tol] ulp " + x + " vs " + y);
            }
        }
        return ok;
    }

    // ---- SCALARS ----------------------------------------------------------

    /** The kind a carrier has at runtime (the engine's runtime kind). */
    static @com.legend.base.Nullable Type carrierKind(@com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> null;
            case BigDecimal ignored -> Type.Primitive.DECIMAL;
            case Double ignored -> Type.Primitive.FLOAT;
            case Float ignored -> Type.Primitive.FLOAT;
            case Long ignored -> Type.Primitive.INTEGER;
            case Integer ignored -> Type.Primitive.INTEGER;
            case Short ignored -> Type.Primitive.INTEGER;
            case Byte ignored -> Type.Primitive.INTEGER;
            case BigInteger ignored -> Type.Primitive.INTEGER;
            case String ignored -> Type.Primitive.STRING;
            case Boolean ignored -> Type.Primitive.BOOLEAN;
            case com.legend.values.PureDateLiteral.StrictDate ignored -> Type.Primitive.STRICT_DATE;
            case com.legend.values.PureDateLiteral ignored -> Type.Primitive.DATE;
            // JDBC temporal carriers (a date literal read back): the same
            // kind on both sides, equal by the carrier's own equality
            case java.time.LocalDate ignored -> Type.Primitive.STRICT_DATE;
            case java.util.Date ignored -> Type.Primitive.DATE;   // the driver's date and timestamp carriers
            case java.time.LocalDateTime ignored -> Type.Primitive.DATE_TIME;
            case java.time.OffsetDateTime ignored -> Type.Primitive.DATE_TIME;
            case java.time.Instant ignored -> Type.Primitive.DATE_TIME;
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
    private static @com.legend.base.Nullable Type effectiveKind(Typed t) {
        Type k = t.kind();
        if (k instanceof Type.PrecisionDecimal) {
            k = Type.Primitive.DECIMAL;
        }
        Type carrier = carrierKind(t.value());
        if ((k == Type.Primitive.FLOAT || k == Type.Primitive.DECIMAL
                || k == Type.Primitive.INTEGER) && isNumericKind(carrier)) {
            return k;
        }
        return carrier;
    }

    private static boolean isNumericKind(@com.legend.base.Nullable Type k) {
        return k == Type.Primitive.FLOAT || k == Type.Primitive.DECIMAL
                || k == Type.Primitive.INTEGER;
    }

    /** Equal, or the first difference. */
    public static @com.legend.base.Nullable String scalar(Typed e, Typed a) {
        return same(e, a) ? null
                : "\nexpected: " + PureAsserts.repr(e.value())
                        + "\nactual:   " + PureAsserts.repr(a.value());
    }

    /** The decision itself (the narrative is the caller's when it has a
     * better one — a row, a path). */
    public static boolean same(Typed et, Typed at) {
        Object e = et.value();
        Object a = at.value();
        if ("TDSNull".equals(e) && a == null) {
            return true;   // the engine's null sentinel in a TDS cell
        }
        if (e == null || a == null) {
            return e == a;
        }
        if ((e instanceof Map<?, ?> || e instanceof List<?>)
                && (a instanceof Map<?, ?> || a instanceof List<?>)) {
            return structural(e, a);
        }
        Type ek = effectiveKind(et);
        Type ak = effectiveKind(at);
        if (ek == null || ak == null) {
            if (e instanceof com.legend.values.PureDateLiteral
                    || a instanceof com.legend.values.PureDateLiteral) {
                return e.equals(a);
            }
            throw new Unjudged(e.getClass().getSimpleName() + " vs "
                    + a.getClass().getSimpleName());
        }
        if (ek != ak) {
            return false;   // Rule 3: a different kind is never equal
        }
        if (ek == Type.Primitive.INTEGER) {
            if (!(isIntegral(e) && isIntegral(a))) {
                return false;   // an Integer-declared non-integral carrier
            }
            return toBigInteger(e).equals(toBigInteger(a));
        }
        if (ek == Type.Primitive.DECIMAL) {
            // the engine's assert seam: getValue().equals — SCALE-SENSITIVE
            // (VERDICT_RULE_AUDIT X2; the equality-worlds fixture pins
            // 3.0D ≠ 3.00D in World 1, while SQL '=' is scale-blind)
            if (!(e instanceof Number en && a instanceof Number an)) {
                return false;
            }
            boolean eq = decimal(en).equals(decimal(an));
            if (!eq && decimal(en).compareTo(decimal(an)) == 0) {
                // leg 3.0 census: the pair differs by SCALE only — the
                // witness set for the canon spec's Decimal amendment
                CanonicalDivergence.decimalScaleOnly();
            }
            return eq;
        }
        if (ek == Type.Primitive.FLOAT) {
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
            return e instanceof Double de && a instanceof Double da
                    && withinTwoUlp(de, da);
        }
        if (ek == Type.Primitive.STRING || ek == Type.Primitive.BOOLEAN) {
            return e.equals(a);
        }
        return e.equals(a);   // dates: the literal's own equality
    }

    private static boolean structural(Object e, Object a) {
        return firstDiff(e, a, "$", (x, y) -> same(Typed.of(x), Typed.of(y))) == null;
    }

    // ---- COLLECTIONS ------------------------------------------------------

    /** Element by element, in order. */
    public static @com.legend.base.Nullable String ordered(List<Typed> e, List<Typed> a) {
        if (e.size() != a.size()) {
            return "\nexpected " + e.size() + " element(s), actual " + a.size();
        }
        for (int i = 0; i < e.size(); i++) {
            if (!same(e.get(i), a.get(i))) {
                return "\nelement " + i + scalar(e.get(i), a.get(i));
            }
        }
        return null;
    }

    /** The engine's {@code assertSameElements}: sort both sides by the
     * value order, then ordered. */
    public static @com.legend.base.Nullable String sameElements(List<Typed> e, List<Typed> a) {
        return ordered(sorted(e), sorted(a));
    }

    /** True when every element of {@code needle} has an equal in
     * {@code hay} (membership, the contains / forAll family). */
    public static boolean contains(List<Typed> hay, Typed needle) {
        for (Typed h : hay) {
            if (same(needle, h)) {
                return true;
            }
        }
        return false;
    }

    /** A grid's flat cells paired with their COLUMN kinds (cycling per
     * row) — the TDS's declared column types decide, cell by cell. */
    public static List<Typed> grid(List<Object> cells, List<Type> columnKinds) {
        int w = columnKinds.size();
        List<Typed> out = new ArrayList<>(cells.size());
        for (int i = 0; i < cells.size(); i++) {
            out.add(new Typed(cells.get(i), w == 0 ? null : columnKinds.get(i % w)));
        }
        return out;
    }

    /** True when the sides are equal under the judge AND every pair that
     * is not identical is a finite Double pair — i.e. the ONLY thing
     * between them is the counted Float leniency. The mixed verdict's
     * arbitration reads this (a byte canon that differs by the last
     * digit while the host holds); it goes with the mixed verdict. */
    public static boolean differByLeniencyOnly(List<Typed> e, List<Typed> a) {
        if (e.isEmpty() || e.size() != a.size() || ordered(e, a) != null) {
            return false;
        }
        for (int i = 0; i < e.size(); i++) {
            Object x = e.get(i).value();
            Object y = a.get(i).value();
            if (java.util.Objects.equals(x, y)) {
                continue;
            }
            if (!(x instanceof Double dx && y instanceof Double dy
                    && Double.isFinite(dx) && Double.isFinite(dy))) {
                return false;
            }
        }
        return true;
    }

    /** Rows of width {@code w} as a MULTISET (each expected row consumes one
     * equal actual row); cells through {@link #same}. */
    public static boolean rowMultiset(List<Typed> e, List<Typed> a, int w) {
        if (e.size() != a.size()) {
            return false;
        }
        List<List<Typed>> pool = new ArrayList<>(chunk(a, w));
        for (List<Typed> row : chunk(e, w)) {
            int hit = -1;
            for (int i = 0; i < pool.size() && hit < 0; i++) {
                if (ordered(row, pool.get(i)) == null) {
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
            default -> String.valueOf(v);   // maps: stable text order
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

    // ---- JSON: the service-test comparator (EqualToJson) -------------------

    /** The engine's service-test {@code JsonNodeComparator}: objects by the
     * union of their keys (null ≡ missing), arrays as multisets, numbers by
     * decimal value (kind-blind) with the referee's 2-ULP policy for the
     * database's C-library doubles, strings and booleans exact. */
    public static @com.legend.base.Nullable String serviceJson(@com.legend.base.Nullable Object expected,
            @com.legend.base.Nullable Object actual) {
        return serviceDiff(expected, actual, "$");
    }

    private static @com.legend.base.Nullable String serviceDiff(@com.legend.base.Nullable Object e,
            @com.legend.base.Nullable Object a, String path) {
        if (e == null && a == null) {
            return null;
        }
        if (e == null || a == null) {
            return path + " expected " + show(e) + ", got " + show(a);
        }
        if (e instanceof Map<?, ?> em && a instanceof Map<?, ?> am) {
            TreeSet<String> keys = new TreeSet<>();
            em.keySet().forEach(k -> keys.add(String.valueOf(k)));
            am.keySet().forEach(k -> keys.add(String.valueOf(k)));
            for (String k : keys) {
                String d = serviceDiff(em.get(k), am.get(k), path + "." + k);
                if (d != null) {
                    return d;
                }
            }
            return null;
        }
        if (e instanceof List<?> el && a instanceof List<?> al) {
            if (el.size() != al.size()) {
                return path + " expected " + el.size() + " elements, got " + al.size();
            }
            List<Object> remaining = new ArrayList<>(al);
            for (int i = 0; i < el.size(); i++) {
                Object x = el.get(i);
                int at = -1;
                for (int j = 0; j < remaining.size() && at < 0; j++) {
                    if (serviceDiff(x, remaining.get(j), path) == null) {
                        at = j;
                    }
                }
                if (at < 0) {
                    Object nearest = nearest(x, remaining);
                    String why = nearest == null ? "no actual element shares its first field"
                            : "nearest actual " + show(nearest) + " differs: "
                                    + serviceDiff(x, nearest, path + "[" + i + "]");
                    return path + "[" + i + "] expected " + show(x)
                            + " has no equal element in the actual array (" + why + ")";
                }
                remaining.remove(at);
            }
            return null;
        }
        if (e instanceof Number en && a instanceof Number an) {
            if (decimal(en).compareTo(decimal(an)) == 0) {
                return null;
            }
            if (withinTwoUlp(en.doubleValue(), an.doubleValue())) {
                return null;
            }
            return path + " expected " + en + ", got " + an;
        }
        if (e instanceof String es && a instanceof String as) {
            return es.equals(as) ? null : path + " expected " + show(es) + ", got " + show(as);
        }
        if (e instanceof Boolean eb && a instanceof Boolean ab) {
            return eb.equals(ab) ? null : path + " expected " + eb + ", got " + ab;
        }
        return path + " expected " + jsonKind(e) + " " + show(e) + ", got " + jsonKind(a) + " " + show(a);
    }

    private static @com.legend.base.Nullable Object nearest(Object expected, List<Object> candidates) {
        if (!(expected instanceof Map<?, ?> em) || em.isEmpty()) {
            return null;
        }
        var first = em.entrySet().iterator().next();
        for (Object c : candidates) {
            if (c instanceof Map<?, ?> cm
                    && serviceDiff(first.getValue(), cm.get(first.getKey()), "$") == null) {
                return c;
            }
        }
        return null;
    }

    private static String jsonKind(Object o) {
        return o instanceof Map ? "object" : o instanceof List ? "array"
                : o instanceof Number ? "number" : o instanceof String ? "string"
                : o instanceof Boolean ? "boolean" : "null";
    }

    private static String show(@com.legend.base.Nullable Object o) {
        if (o == null) {
            return "null";
        }
        String s = o instanceof String str ? "'" + str + "'" : String.valueOf(o);
        return s.length() > 160 ? s.substring(0, 157) + "..." : s;
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
