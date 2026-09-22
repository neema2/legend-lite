// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;

import com.legend.compiler.element.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * X5 — a class's {@code <<equality.Key>>} identity, resolved from the
 * model at COMPILE TIME (the engine's
 * {@code _Class.getEqualityKeyProperties}: simple properties carrying
 * the stereotype, own class first then supertypes, name-deduped).
 * Instance equality over a keyed class compares KEY PROPERTIES ONLY
 * ({@code EqualityUtilities.equal}); a keyless class refuses value
 * equality entirely (identity or FALSE) — {@code resolve} returns null
 * for it, and every consumer declines rather than judging.
 *
 * <p>{@code nested} carries a class-typed key's own key tree (the
 * engine recurses through {@code equal}); a class-typed key whose
 * class is KEYLESS poisons the whole resolution (equality through it
 * can never hold by value) — null, decline. Cycles likewise.
 */
public record EqualityKeys(String classFqn, List<Key> keys) {

    /** One key property: {@code many} marks a to-many key
     * ({@code List.values : T[*]} — engine compares the value
     * COLLECTIONS under the ordered list rule). */
    public record Key(String name, boolean many,
                      @com.legend.base.Nullable EqualityKeys nested) {
    }

    /** The class FQN a stamp names, or null for non-class stamps —
     * bare {@code ClassType} and parameterized {@code GenericType}
     * both name a classifier (the engine's classifier-match rule reads
     * the raw class; type arguments never change WHICH keys exist). */
    public static @com.legend.base.Nullable String fqnOf(Type t) {
        if (t instanceof Type.ClassType ct) {
            return ct.fqn();
        }
        if (t instanceof Type.GenericType gt) {
            return gt.rawFqn();
        }
        return null;
    }

    public static @com.legend.base.Nullable EqualityKeys resolve(
            ModelContext ctx, String classFqn) {
        return resolve(ctx, classFqn, java.util.Map.of(),
                new LinkedHashSet<>());
    }

    /** SUBSTITUTION-AWARE resolution (the Pair-of-Pairs fix): a
     * PARAMETERIZED stamp instantiates its type arguments into the key
     * property types before class-nesting is decided — {@code Pair<U,V>}
     * declares {@code first: U[1]}, so the DECLARED type never nests;
     * the stamp {@code Pair<Pair<Integer,String>, X>} does (witness the
     * zip PCT family's three instance-key-shape declines). */
    public static @com.legend.base.Nullable EqualityKeys resolve(
            ModelContext ctx, Type t) {
        String fqn = fqnOf(t);
        if (fqn == null) {
            return null;
        }
        return resolve(ctx, fqn, typeArgsOf(ctx, t), new LinkedHashSet<>());
    }

    private static @com.legend.base.Nullable EqualityKeys resolve(
            ModelContext ctx, String classFqn, Map<String, Type> typeArgs,
            Set<String> inProgress) {
        // the cycle guard keys the INSTANTIATION, not the bare class —
        // Pair-inside-Pair is a different instantiation and terminates
        // structurally (arguments shrink); the SAME instantiation
        // revisited is a genuine key cycle
        String guard = classFqn + typeArgs;
        if (!inProgress.add(guard)) {
            return null;   // key cycle — never claimable by value
        }
        try {
            List<Key> keys = new ArrayList<>();
            Set<String> seen = new LinkedHashSet<>();
            if (!collect(ctx, classFqn, typeArgs, inProgress, keys, seen,
                    new LinkedHashSet<>())) {
                return null;
            }
            return keys.isEmpty() ? null : new EqualityKeys(classFqn, keys);
        } finally {
            inProgress.remove(guard);
        }
    }

    /** The stamp's positional type-argument map ({@link ClassLayouts}'
     * generic-instantiation rule), empty for bare classes or malformed
     * parameterizations. */
    private static Map<String, Type> typeArgsOf(ModelContext ctx, Type t) {
        if (!(t instanceof Type.GenericType g)) {
            return java.util.Map.of();
        }
        TypedClass c = ctx.findClass(g.rawFqn()).orElse(null);
        if (c == null || c.typeParameters().size() != g.arguments().size()) {
            return java.util.Map.of();
        }
        Map<String, Type> args = new java.util.LinkedHashMap<>();
        for (int i = 0; i < c.typeParameters().size(); i++) {
            args.put(c.typeParameters().get(i), g.arguments().get(i));
        }
        return args;
    }

    /** Positional substitution of type-parameter occurrences (the
     * {@link ClassLayouts} rule, applied to KEY property types). */
    private static Type substitute(Type t, Map<String, Type> typeArgs) {
        return switch (t) {
            case Type.TypeVar v -> typeArgs.getOrDefault(v.name(), t);
            case Type.GenericType g -> new Type.GenericType(g.rawFqn(),
                    g.arguments().stream()
                            .map(a -> substitute(a, typeArgs)).toList());
            default -> t;
        };
    }

    /** Walks {@code fqn} then its supertypes (engine generalization
     * order), appending keyed stored properties. The engine's key set
     * is the class's SIMPLE PROPERTIES filtered by the stereotype
     * (_Class.collectEqualityKeyProperties), so a subclass
     * REDECLARATION shadows the inherited property whether or not the
     * redeclaration is keyed — an un-keyed redeclaration REMOVES the
     * super's key (witness testEqualNonPrimitive's OtherBottomClass:
     * {@code sides} redeclared bare, {@code otherBot11 == otherBot21}
     * with differing sides holds). False = poisoned (a class-typed key
     * over a keyless class). */
    private static boolean collect(ModelContext ctx, String fqn,
            Map<String, Type> typeArgs, Set<String> inProgress,
            List<Key> out, Set<String> seenNames, Set<String> seenClasses) {
        if (!seenClasses.add(fqn)) {
            return true;   // diamond — already contributed
        }
        TypedClass tc = ctx.findClass(fqn).orElse(null);
        if (tc == null) {
            return true;   // unknown super — contributes nothing
        }
        for (Property p : tc.properties()) {
            if (!(p instanceof Property.Stored st)
                    || !seenNames.add(st.name()) || !st.equalityKey()) {
                continue;
            }
            Type pt = substitute(st.type(), typeArgs);
            // a class-typed key slot: the DECLARED class's keys when it has
            // them; a keyless declared class (an abstract polymorphic slot —
            // DynaFunction.parameters : RelationalOperationElement[*]) leaves
            // nested = null and the VALUE is judged by its own classifier
            // (engine equal() recurses per value; WORLD_MAP §4 — the wire
            // carries the class as __type). A keyless runtime class then
            // compares by identity, exactly as before.
            EqualityKeys nested = null;
            if (pt instanceof Type.ClassType ct) {
                nested = resolve(ctx, ct.fqn(), java.util.Map.of(),
                        inProgress);
            } else if (pt instanceof Type.GenericType g) {
                nested = resolve(ctx, g.rawFqn(), typeArgsOf(ctx, g),
                        inProgress);
            }
            out.add(new Key(st.name(), st.multiplicity().isMany(), nested));
        }
        for (String sup : tc.superClassFqns()) {
            if (!collect(ctx, sup, typeArgs, inProgress, out, seenNames,
                    seenClasses)) {
                return false;
            }
        }
        return true;
    }
}
