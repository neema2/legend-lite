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

    /** The types {@code s}'s values are seen to have: its own; where it is typed Any, its inputs' -- a cast's source, a
     *  collection literal's members, an if's branches, a collection function's list and values ({@code first}, {@code
     *  at}, {@code filter}, {@code add}, {@code concatenate}), a map's body -- each read the same way (a Boolean input, a
     *  condition or a predicate, contributes none); a property read off an object literal, the value it was given; an
     *  Any whose values come from nowhere the plan holds (a variable, a stored property), itself. An empty literal
     *  contributes none. (The final audit of c758fe95, 2026-11-23: reading only casts, literals and concatenations
     *  let {@code [fromJson('1'), 2]->first()} and {@code if(c, |fromJson('1'), |2)} reach the database's own
     *  equality.) */
    public static Set<Type> seen(TypedSpec s) {
        Set<Type> out = new LinkedHashSet<>();
        collect(s, out);
        return out;
    }

    private static void collect(TypedSpec s, Set<Type> out) {
        Type t = s.info().type();
        if (s instanceof TypedCollection tc) {
            tc.elements().forEach(e -> collect(e, out));
        } else if (s instanceof TypedLambda l) {
            if (!l.body().isEmpty()) {
                collect(l.body().get(l.body().size() - 1), out);
            }
        } else if (PlatformTypes.isAny(t) && s instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedNewInstance ni && ni.properties().get(pa.property()) != null) {
            collect(ni.properties().get(pa.property()), out);
        } else if (PlatformTypes.isAny(t) && !(s instanceof TypedPropertyAccess) && !s.children().isEmpty()) {
            for (TypedSpec c : s.children()) {
                if (!condition(c)) {
                    collect(c, out);
                }
            }
        } else if (!PlatformTypes.isNil(t)) {
            out.add(t);
        }
    }

    /** A Boolean input: a condition or a predicate, never a value an Any passes on. */
    private static boolean condition(TypedSpec c) {
        TypedSpec v = c instanceof TypedLambda l && !l.body().isEmpty() ? l.body().get(l.body().size() - 1) : c;
        return v.info().type() == Type.Primitive.BOOLEAN;
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
        if (sa.stream().anyMatch(VariantEquality::nestedVariant) || sb.stream().anyMatch(VariantEquality::nestedVariant)) {
            // a Pair, a List or a Map holding a Variant: comparing it compares the Variant within
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
        if (seen.stream().anyMatch(VariantEquality::holdsVariant)) {
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
