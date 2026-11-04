// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * THE free-variable function of the typed tree (rebuild W0.6 push 1): the
 * names a term reads that no binder inside the term binds. A substitution
 * is capture-avoiding exactly when no binder it passes under is a free
 * variable of a term it substitutes, so every substitution engine over the
 * typed tree asks this one function.
 *
 * <p>Binders: a {@link TypedLambda}'s parameters (over its body); a
 * {@link TypedLet} (over the LATER statements of the same body, never its
 * own value); {@link TypedMatch#param()} and {@code extraParam()} (over the
 * body, not the input or the extra argument); each
 * {@link TypedMatchRuntime.Arm}'s parameter and the node's
 * {@code extraParam} (over every arm body, not the input, the extra
 * argument or the dynamic arm prefix).
 *
 * <p>{@link TypedSerializeGraph#rowVar()} is deliberately NOT a binder
 * here: a name reported free that is in fact bound costs one rename; a
 * name reported bound that is in fact free is a capture.
 */
public final class FreeVars {

    private FreeVars() {
    }

    /** The free variables of one term. */
    public static Set<String> of(TypedSpec term) {
        Set<String> out = new LinkedHashSet<>();
        walk(term, Set.of(), out);
        return out;
    }

    /** {@link #of} of a term whose subterms are SHARED (a let's value spliced at every read, the inliner's): each node's
     *  free variables computed once, bottom-up, and kept in {@code memo} by identity -- the same answer as {@link #of},
     *  in time of the distinct nodes, where {@link #of} walks every read again (2026-11-04, a relation read more than
     *  once lowered once, {@code lowering.SharedRelations}). The same binders as {@link #of}'s walk. */
    public static Set<String> ofShared(TypedSpec term, java.util.Map<TypedSpec, Set<String>> memo) {
        Set<String> known = memo.get(term);
        if (known != null) {
            return known;
        }
        Set<String> out = new LinkedHashSet<>();
        switch (term) {
            case TypedVariable v -> out.add(v.name());
            case TypedLambda l -> {
                Set<String> scope = new LinkedHashSet<>(l.parameters());
                for (TypedSpec st : l.body()) {
                    for (String f : ofShared(st, memo)) {
                        if (!scope.contains(f)) {
                            out.add(f);
                        }
                    }
                    if (st instanceof TypedLet let) {
                        scope.add(let.name());
                    }
                }
            }
            case TypedMatch m -> {
                out.addAll(ofShared(m.input(), memo));
                m.extra().ifPresent(e -> out.addAll(ofShared(e, memo)));
                for (String f : ofShared(m.body(), memo)) {
                    if (!f.equals(m.param()) && !m.extraParam().map(f::equals).orElse(false)) {
                        out.add(f);
                    }
                }
            }
            case TypedMatchRuntime mr -> {
                out.addAll(ofShared(mr.input(), memo));
                mr.extra().ifPresent(e -> out.addAll(ofShared(e, memo)));
                mr.dynamicArms().ifPresent(d -> out.addAll(ofShared(d, memo)));
                for (TypedMatchRuntime.Arm a : mr.arms()) {
                    for (String f : ofShared(a.body(), memo)) {
                        if (!f.equals(a.param()) && !mr.extraParam().map(f::equals).orElse(false)) {
                            out.add(f);
                        }
                    }
                }
            }
            default -> {
                for (TypedSpec c : term.children()) {
                    out.addAll(ofShared(c, memo));
                }
            }
        }
        Set<String> result = java.util.Collections.unmodifiableSet(out);
        memo.put(term, result);
        return result;
    }

    /** The free variables of {@code term}, given that its subterm
     * {@code known} (the same node, wherever it stands) has the free
     * variables {@code knownFree}: the walk does not enter it. A term
     * grown step by step around its previous value (an unrolled fold's
     * accumulator) is then read in the size of the step, not of the
     * whole. */
    public static Set<String> of(TypedSpec term, TypedSpec known, Set<String> knownFree) {
        Set<String> out = new LinkedHashSet<>();
        new Walk(known, knownFree).walk(term, Set.of(), out);
        return out;
    }

    /** The union over INDEPENDENT terms (an argument list, the values of
     * an environment): no term binds for another. */
    public static Set<String> of(Collection<? extends TypedSpec> terms) {
        Set<String> out = new LinkedHashSet<>();
        for (TypedSpec t : terms) {
            walk(t, Set.of(), out);
        }
        return out;
    }

    /** The free variables of a STATEMENT SEQUENCE: a let binds its name
     * for the statements after it. */
    public static Set<String> ofBody(List<? extends TypedSpec> statements) {
        Set<String> out = new LinkedHashSet<>();
        sequence(statements, Set.of(), out);
        return out;
    }

    /** Every name a binder beneath {@code term} introduces (a renaming
     * picks a name outside this set, so the new name is never re-bound
     * below). */
    public static Set<String> binders(TypedSpec term) {
        Set<String> out = new LinkedHashSet<>();
        binders(term, out);
        return out;
    }

    /** The name of every {@link TypedLet} beneath {@code term}. */
    public static Set<String> lets(TypedSpec term) {
        Set<String> out = new LinkedHashSet<>();
        lets(term, out);
        return out;
    }

    private static void lets(TypedSpec n, Set<String> out) {
        if (n instanceof TypedLet let) {
            out.add(let.name());
        }
        for (TypedSpec c : n.children()) {
            lets(c, out);
        }
    }

    private static void binders(TypedSpec n, Set<String> out) {
        switch (n) {
            case TypedLambda l -> out.addAll(l.parameters());
            case TypedLet let -> out.add(let.name());
            case TypedMatch m -> {
                out.add(m.param());
                m.extraParam().ifPresent(out::add);
            }
            case TypedMatchRuntime mr -> {
                mr.arms().forEach(a -> out.add(a.param()));
                mr.extraParam().ifPresent(out::add);
            }
            default -> { }
        }
        for (TypedSpec c : n.children()) {
            binders(c, out);
        }
    }

    private static void sequence(List<? extends TypedSpec> statements, Set<String> bound,
            Set<String> out) {
        PLAIN.sequence(statements, bound, out);
    }

    private static void walk(TypedSpec n, Set<String> bound, Set<String> out) {
        PLAIN.walk(n, bound, out);
    }

    private static final Walk PLAIN = new Walk(null, Set.of());

    /** One walk; {@code known} is the subterm it does not enter (none
     * when null). */
    private record Walk(@com.legend.base.Nullable TypedSpec known, Set<String> knownFree) {

        void sequence(List<? extends TypedSpec> statements, Set<String> bound, Set<String> out) {
            Set<String> scope = bound;
            for (TypedSpec st : statements) {
                walk(st, scope, out);
                if (st instanceof TypedLet let) {
                    scope = with(scope, List.of(let.name()));
                }
            }
        }

        void walk(TypedSpec n, Set<String> bound, Set<String> out) {
            if (n == known) {
                for (String v : knownFree) {
                    if (!bound.contains(v)) {
                        out.add(v);
                    }
                }
                return;
            }
            switch (n) {
                case TypedVariable v -> {
                    if (!bound.contains(v.name())) {
                        out.add(v.name());
                    }
                }
                case TypedLambda l -> sequence(l.body(), with(bound, l.parameters()), out);
                case TypedMatch m -> {
                    walk(m.input(), bound, out);
                    m.extra().ifPresent(e -> walk(e, bound, out));
                    Set<String> inner = with(bound, List.of(m.param()));
                    walk(m.body(), m.extraParam().map(x -> with(inner, List.of(x))).orElse(inner), out);
                }
                case TypedMatchRuntime mr -> {
                    walk(mr.input(), bound, out);
                    mr.extra().ifPresent(e -> walk(e, bound, out));
                    mr.dynamicArms().ifPresent(d -> walk(d, bound, out));
                    Set<String> shared =
                            mr.extraParam().map(x -> with(bound, List.of(x))).orElse(bound);
                    for (TypedMatchRuntime.Arm a : mr.arms()) {
                        walk(a.body(), with(shared, List.of(a.param())), out);
                    }
                }
                default -> {
                    for (TypedSpec c : n.children()) {
                        walk(c, bound, out);
                    }
                }
            }
        }
    }

    private static Set<String> with(Set<String> bound, Collection<String> names) {
        if (bound.containsAll(names)) {
            return bound;
        }
        Set<String> out = new LinkedHashSet<>(bound);
        out.addAll(names);
        return out;
    }
}
