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

    /** The types {@code s}'s values are seen to have: its own; through a cast to Any, the cast value's; a collection
     *  literal's or a concatenation's, its members'. An empty literal contributes none. */
    public static Set<Type> seen(TypedSpec s) {
        Set<Type> out = new LinkedHashSet<>();
        collect(s, out);
        return out;
    }

    private static void collect(TypedSpec s, Set<Type> out) {
        if (s instanceof TypedCast c && PlatformTypes.isAny(s.info().type())) {
            collect(c.source(), out);
        } else if (s instanceof TypedCollection tc) {
            tc.elements().forEach(e -> collect(e, out));
        } else if (s instanceof TypedNativeCall n
                && com.legend.builtin.Pure.AT_COLLECTION_CONCATENATE.contains(n.callee().id())) {
            n.args().forEach(a -> collect(a, out));
        } else if (!PlatformTypes.isNil(s.info().type())) {
            out.add(s.info().type());
        }
    }

    /** A side's one type for the judges' facts: the Variant an Any side is seen to be, else its declared type. */
    public static Type sideType(TypedSpec s) {
        Set<Type> seen = seen(s);
        return PlatformTypes.isAny(s.info().type()) && seen.size() == 1
                && PlatformTypes.isVariant(seen.iterator().next()) ? seen.iterator().next() : s.info().type();
    }

    /** An equality between {@code a}'s values and {@code b}'s: refused by name where only identity decides, DISJOINT
     *  where a Variant meets values of other types alone, else COMPUTED. */
    public static Verdict between(TypedSpec a, TypedSpec b) {
        Set<Type> sa = seen(a);
        Set<Type> sb = seen(b);
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

    /** Comparisons among one collection's own values (removeDuplicates, distinct, isDistinct, union, a dedup's key):
     *  refused by name where a Variant is seen among them. */
    public static void refuseAmong(TypedSpec values) {
        Set<Type> seen = seen(values);
        if (seen.stream().anyMatch(PlatformTypes::isVariant)) {
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
