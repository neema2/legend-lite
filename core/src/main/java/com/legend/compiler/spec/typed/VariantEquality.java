package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Register S401 (ruled by the user 2026-11-23, "Okay"): legend-pure compares Variants by identity (two separately
 * built Variants are never equal, asked of its compiled runtime), which a database cannot compute. One reading of an
 * equality's two sides, for the product's equalities and both judges alike: the types a side's values are SEEN to
 * have -- its own, looking through a cast to Any and into a collection literal's (or a concatenation's) members (the
 * audit of 581a93cb and 386db8fe, 2026-11-23: reading the declared type alone let {@code [fromJson('1'), 2]}, an
 * {@code Any[*]}, reach the database's JSON equality). Where a Variant meets values of other types alone, the
 * equality is decided by the types: not equal. Where a Variant may meet a Variant or a value the plan cannot see, or
 * sits among values of other types the database would compare it with, only identity decides: refused by name.
 */
public final class VariantEquality {

    private VariantEquality() {
    }

    /** How an equality between two sides' values is decided. */
    public enum Verdict {
        /** no Variant among either side's values: the comparison is computed as before */
        COMPUTED,
        /** a side's values are all Variants and the other side's are values of other types alone: never equal */
        DISJOINT
    }

    /** The types {@code s}'s values are seen to have: its own; where it is typed Any, the inputs that pass a value on
     *  -- a cast's source, a collection literal's members, an if's branches (never its condition), a filter's, sort's,
     *  take's or slice's list (never its predicate, key or count), a map's body (its parameter read as the list's
     *  values), a native call's arguments whose declared parameter carries the call's own type variable ({@code at}'s
     *  list, never its index; {@code add}'s list and value), any other node's inputs -- each read the same way; a
     *  property read off an object literal, the value it was given; an Any whose values come from nowhere the plan
     *  holds (a variable, a stored property), itself. An empty literal contributes none. (The final audit of
     *  c758fe95, 2026-11-23: reading only casts, literals and concatenations let {@code [fromJson('1'), 2]->first()}
     *  and {@code if(c, |fromJson('1'), |2)} reach the database's own equality; the narrow review of f9e74256: skipping
     *  every Boolean input dropped an if's {@code |true} branch, and counting an index or a count refused
     *  {@code ->at(0)}.) */
    public static Set<Type> seen(TypedSpec s) {
        Set<Type> out = new LinkedHashSet<>();
        collect(s, out, java.util.Map.of(), PlatformTypes::isAny, true);
        return out;
    }

    /** The kinds of date {@code s}'s values are seen to have, read through a node typed Any or the abstract Date (a
     *  list literal of days and date-times is typed Date, the database carrying it as one kind: register S399) where
     *  it passes its values on unchanged -- a literal's members, an if's branches, a filter's, sort's, take's or
     *  slice's list, a map's body, a cast, a relation's concatenation -- never into a function's arguments (a list's
     *  {@code concatenate} and {@code add} included, native calls): a function's result is
     *  the value the database computes, its kind the plan's (round 8 of the narrow review's fixes: reading max's
     *  arguments refused PCT's max, min, greatest and least over days and date-times, which lite answers). */
    public static Set<Type> dateKinds(TypedSpec s) {
        Set<Type> out = new LinkedHashSet<>();
        collect(s, out, java.util.Map.of(), t -> PlatformTypes.isAny(t) || t == Type.Primitive.DATE, false);
        return out;
    }

    private static void collect(TypedSpec s, Set<Type> out, java.util.Map<String, Set<Type>> bound,
            java.util.function.Predicate<Type> open, boolean calls) {
        Type t = s.info().type();
        if (s instanceof TypedCollection tc) {
            tc.elements().forEach(e -> collect(e, out, bound, open, calls));
        } else if (s instanceof TypedLambda l) {
            if (!l.body().isEmpty()) {
                collect(l.body().get(l.body().size() - 1), out, bound, open, calls);
            }
        } else if (!open.test(t)) {
            if (!PlatformTypes.isNil(t)) {
                out.add(t);
            }
        } else if (s instanceof TypedVariable v && bound.containsKey(v.name())) {
            out.addAll(bound.get(v.name()));
        } else if (s instanceof TypedMap m) {
            java.util.Map<String, Set<Type>> inner = new java.util.HashMap<>(bound);
            if (!m.mapper().parameters().isEmpty()) {
                Set<Type> source = new LinkedHashSet<>();
                collect(m.source(), source, bound, open, calls);
                inner.put(m.mapper().parameters().get(0), source);
            }
            collect(m.mapper(), out, inner, open, calls);
        } else if (s instanceof TypedPropertyAccess pa) {
            if (pa.source() instanceof TypedNewInstance ni && ni.properties().get(pa.property()) != null) {
                collect(ni.properties().get(pa.property()), out, bound, open, calls);
            } else {
                out.add(t);
            }
        } else {
            java.util.List<TypedSpec> values = values(s, calls);
            if (values.isEmpty()) {
                out.add(t);
            }
            values.forEach(c -> collect(c, out, bound, open, calls));
        }
    }

    /** The inputs of an Any-typed node that pass a value on; with {@code calls} false, only those passed on unchanged
     *  (a function's arguments, and any other node's inputs, read as none: the node is its own value). */
    private static java.util.List<TypedSpec> values(TypedSpec s, boolean calls) {
        return switch (s) {
            case TypedIf i -> i.elseBranch().map(e -> java.util.List.of(i.thenBranch(), e))
                    .orElseGet(() -> java.util.List.of(i.thenBranch()));
            case TypedFilter f -> java.util.List.of(f.source());
            case TypedSort o -> java.util.List.of(o.source());
            case TypedLimit l -> java.util.List.of(l.source());
            case TypedDrop d -> java.util.List.of(d.source());
            case TypedSlice l -> java.util.List.of(l.source());
            case TypedCast c -> java.util.List.of(c.source());
            case TypedConcatenate c -> java.util.List.of(c.left(), c.right());
            case TypedNativeCall n -> calls ? carried(n) : java.util.List.of();
            default -> calls ? s.children() : java.util.List.of();
        };
    }

    /** A native call's arguments whose declared parameter carries the call's own type variable (a value of it, a
     *  list of it, a function returning it); every argument where the signature does not say. */
    private static java.util.List<TypedSpec> carried(TypedNativeCall n) {
        java.util.List<com.legend.compiler.element.TypedParameter> ps = n.callee().parameters();
        if (!(n.callee().returnType() instanceof Type.TypeVar tv) || ps.size() != n.args().size()) {
            return n.args();
        }
        java.util.List<TypedSpec> out = new java.util.ArrayList<>();
        for (int i = 0; i < ps.size(); i++) {
            if (mentions(ps.get(i).type(), tv)) {
                out.add(n.args().get(i));
            }
        }
        return out;
    }

    private static boolean mentions(Type t, Type.TypeVar tv) {
        return t.equals(tv)
                || t instanceof Type.FunctionType f && mentions(f.result().type(), tv)
                || t instanceof Type.GenericType g && g.arguments().stream().anyMatch(a -> mentions(a, tv));
    }

    /** Whether {@code t} holds a Variant: is one, or a generic type (a Pair, a List, a Map) one of whose arguments
     *  does -- comparing two such values compares their Variants. */
    public static boolean holdsVariant(Type t) {
        return PlatformTypes.isVariant(t)
                || t instanceof Type.GenericType g && g.arguments().stream().anyMatch(VariantEquality::holdsVariant);
    }

    /** A side's one type for the judges' facts: the Variant, or the enumeration, an Any side is seen to be (a value of
     *  a kind of its own, never a String of its text: S401, S169), else its declared type. */
    public static Type sideType(TypedSpec s) {
        Set<Type> seen = seen(s);
        Type one = seen.size() == 1 ? seen.iterator().next() : null;
        return PlatformTypes.isAny(s.info().type()) && one != null
                && (PlatformTypes.isVariant(one) || one instanceof Type.EnumType) ? one : s.info().type();
    }

    /** An equality between {@code a}'s values and {@code b}'s: refused by name where only identity decides, DISJOINT
     *  where a Variant meets values of other types alone, else COMPUTED. */
    public static Verdict between(TypedSpec a, TypedSpec b) {
        Set<Type> sa = seen(a);
        Set<Type> sb = seen(b);
        if (sa.stream().anyMatch(VariantEquality::nestedVariant) || sb.stream().anyMatch(VariantEquality::nestedVariant)
                || heldVariant(a) || heldVariant(b)) {
            // a Pair, a List, a Map or an object holding a Variant (by its type, or in a member the plan builds, an Any
            // member's too): comparing it compares the Variant within
            throw refused();
        }
        boolean va = sa.stream().anyMatch(PlatformTypes::isVariant);
        boolean vb = sb.stream().anyMatch(PlatformTypes::isVariant);
        if (!va && !vb) {
            return Verdict.COMPUTED;
        }
        if (va && (vb || sb.stream().anyMatch(PlatformTypes::isAny)) || vb && sa.stream().anyMatch(PlatformTypes::isAny)) {
            throw refused();
        }
        Set<Type> variants = va ? sa : sb;
        Set<Type> others = va ? sb : sa;
        if (others.isEmpty() || others.stream().anyMatch(PlatformTypes::isTdsNull)) {
            // an empty value (x == [] is the absence test), or the TDSNull sentinel's own lane
            return Verdict.COMPUTED;
        }
        if (variants.size() > 1) {
            // a Variant among values of other types: the database would compare it with the other side's values
            throw refused();
        }
        return Verdict.DISJOINT;
    }

    /** Whether a value the plan builds -- a pair, a list or a map, reached through a collection literal, a cast or an
     *  if -- holds a Variant among its members' values, at any depth: a member declared Any whose value is a Variant
     *  too (the narrow review of f9e74256: {@code pair(1, fromJson('1')->cast(@Any))} had been compared by the
     *  database). Never an object literal's properties: a keyed class compares by its keys alone and a keyless one by
     *  its identity (S111), so a Variant in another property is never compared (the review of a39883f3: reading every
     *  property refused them); an object whose Any KEY holds a Variant is a recorded gap (S401). */
    private static boolean heldVariant(TypedSpec s) {
        return switch (s) {
            case TypedNativeCall n when n.info().type() instanceof Type.GenericType ->
                    n.args().stream().anyMatch(VariantEquality::holds);
            case TypedCollection c -> c.elements().stream().anyMatch(VariantEquality::heldVariant);
            case TypedCast c -> heldVariant(c.source());
            case TypedIf i -> heldVariant(i.thenBranch()) || i.elseBranch().map(VariantEquality::heldVariant).orElse(false);
            default -> false;
        };
    }

    private static boolean holds(TypedSpec member) {
        return seen(member).stream().anyMatch(VariantEquality::holdsVariant) || heldVariant(member);
    }

    private static boolean nestedVariant(Type t) {
        return !PlatformTypes.isVariant(t) && holdsVariant(t);
    }

    /** A keyed class whose equality keys hold a Variant (a key declared Variant, or a nested key's): comparing two of
     *  its objects compares the Variants, refused by name. */
    public static void refuseVariantKeys(com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys) {
        if (keys == null) {
            return;
        }
        for (com.legend.compiler.element.EqualityKeys.Key k : keys.keys()) {
            if (k.declaredClass() != null && PlatformTypes.isVariant(new Type.ClassType(k.declaredClass()))) {
                throw refused();
            }
            refuseVariantKeys(k.nested());
        }
    }

    /** Comparisons among one collection's own values (removeDuplicates, distinct, isDistinct, union, a dedup's key):
     *  refused by name where a Variant is seen among them. */
    public static void refuseAmong(TypedSpec values) {
        Set<Type> seen = seen(values);
        if (seen.stream().anyMatch(VariantEquality::holdsVariant) || heldVariant(values)) {
            throw refused();
        }
    }

    /** Whether a side's values may be none (its lower bound is zero). */
    public static boolean mayBeEmpty(TypedSpec s) {
        return !(s.info().multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.lower() >= 1);
    }

    private static RuntimeException refused() {
        return new com.legend.error.NotImplementedException(PlatformTypes.VARIANT_EQUALITY_REFUSED);
    }
}
