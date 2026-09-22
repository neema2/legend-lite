// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.model.Function;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;

import java.util.ArrayList;
import java.util.List;

/**
 * THE engine function id, GENERATED from a declaration — never parsed
 * back out of a string. Real pure identifies an overload by the id its
 * {@code FunctionDescriptor} spells (legend-pure m3 navigation/function/
 * FunctionDescriptor.java:196–232): the qualified name, then one
 * {@code _Type_mult_} segment per parameter and one for the return type,
 * where a type is its raw simple name and a multiplicity is {@code 1}
 * (exact), {@code MANY} ({@code *}), {@code $lo_MANY$} ({@code lo..*}),
 * {@code $lo_hi$} (a range) or the multiplicity PARAMETER's own name
 * ({@code sortBy<T,U|m>(col:T[m], …)} is {@code sortBy_T_m__…}). A
 * reference such as {@code sortBy_T_m__Function_$0_1$__T_m_} resolves by
 * spelling each declaration under a prefix of it (the base as the reference
 * spells it, bare or qualified, plus the declaration's tail) and keeping the
 * EXACT match — a spelling this platform cannot reproduce is a miss, loud at
 * the caller, never a redirect (text-surgery audit §1.1 #4; the previous
 * regex decoder guessed the grammar and missed every multiplicity
 * parameter, Phase 5 batch 147).
 */
public final class SignatureMangle {

    private SignatureMangle() {
    }

    /** The signature TAIL of {@code def} — its engine id without the qualified
     * name (a reference spells the base as its author did, bare or
     * qualified; the tail is what the declaration contributes). */
    public static String tail(Function def) {
        return mangle(def).substring(def.qualifiedName().length());
    }

    /** The engine id of {@code def}. */
    public static String mangle(Function def) {
        StringBuilder id = new StringBuilder(def.qualifiedName());
        if (def.parameters().isEmpty()) {
            id.append('_');
        }
        for (var p : def.parameters()) {
            segment(id, p.type(), p.multiplicity());
        }
        segment(id, def.returnType(), def.returnMultiplicity());
        return id.toString();
    }

    private static void segment(StringBuilder id, TypeExpression type, Multiplicity mult) {
        id.append('_').append(typeId(type)).append('_').append(multId(mult)).append('_');
    }

    /** The raw simple name — a type parameter is its own name; a function
     * or relation type is its m3 metaclass. */
    private static String typeId(TypeExpression t) {
        String q = switch (t) {
            case TypeExpression.NameRef n -> n.name();
            case TypeExpression.Generic g -> g.name();
            case TypeExpression.FunctionType f -> "Function";
            case TypeExpression.RelationType r -> "Relation";
            case TypeExpression.SchemaAlgebra s -> "Relation";
        };
        int cut = q.lastIndexOf("::");
        return cut < 0 ? q : q.substring(cut + 2);
    }

    private static String multId(Multiplicity m) {
        return switch (m) {
            case Multiplicity.Parameter p -> p.name();
            case Multiplicity.Concrete c -> {
                if (c.upperBound() == null) {
                    yield c.lowerBound() == 0 ? "MANY" : "$" + c.lowerBound() + "_MANY$";
                }
                int upper = c.upperBound();
                yield c.lowerBound() == upper ? Integer.toString(upper)
                        : "$" + c.lowerBound() + "_" + upper + "$";
            }
        };
    }

    /** The outcome of resolving a reference: the declarations whose engine
     * id is exactly {@code ref}, and whether SOME declaration exists under a
     * prefix of it (a base this platform spells differently — the caller
     * decides what an opaque reference to it means). */
    public record Resolution<F>(List<F> exact, boolean baseExists) {
    }

    /**
     * Resolve a possibly-mangled reference against declarations looked up
     * by base name: every {@code _} in the reference's last segment is a
     * candidate cut, {@code lookup} returns the declarations under that
     * base, {@code def} their parser definition. A plain (unmangled) name
     * resolves through the same loop at its full length.
     */
    public static <F> Resolution<F> resolve(String ref,
            java.util.function.Function<String, List<F>> lookup,
            java.util.function.Function<F, com.legend.model.@com.legend.base.Nullable Function> def) {
        int from = Math.max(ref.lastIndexOf("::") + 2, 0);
        boolean baseExists = false;
        for (int i = ref.length() - 1; i > from; i--) {
            if (ref.charAt(i) != '_') {
                continue;
            }
            List<F> cands = lookup.apply(ref.substring(0, i));
            if (cands.isEmpty()) {
                continue;
            }
            baseExists = true;
            List<F> exact = new ArrayList<>();
            for (F c : cands) {
                Function d = def.apply(c);
                if (d != null && ref.equals(ref.substring(0, i) + tail(d))) {
                    exact.add(c);
                }
            }
            if (!exact.isEmpty()) {
                return new Resolution<>(exact, true);
            }
        }
        return new Resolution<>(List.of(), baseExists);
    }
}
