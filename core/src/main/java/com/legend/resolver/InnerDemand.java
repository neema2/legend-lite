// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * INNER-PREDICATE DEMAND over a navigation head: which lambdas run against
 * the head's TARGET rows, and which of the target's own properties they
 * read. Feeds the target materialization (demanded leaves whose bindings
 * read the target's join slots pull those slots' LEFT joins — W4) and the
 * nested-scope registries (R1 recursive demand).
 *
 * <p>Two consumer shapes contribute, both keyed on the head path over the
 * ENCLOSING lambda's own variable:
 * <ul>
 *   <li>{@code $p.head->filter(f)->exists(g)}-family — a native call whose
 *       receiver is a filter chain over the head (the emptiness family);</li>
 *   <li>{@code $p.head->filter(f)->toOne().leaf} — FILTERED NAVIGATION
 *       consumed as a VALUE (the qualifier family; the shape
 *       {@code Substitution.filteredNavLeafRead} rewrites). Its predicate
 *       reads land on the target pipeline, so its class-typed slot hops
 *       must be demanded here or they die at the rewritePath slot wall.</li>
 * </ul>
 *
 * <p>Every op-level lambda is scanned with its OWN parameter as the
 * instance variable — filter predicates AND the map/project lambdas that
 * hold value-position reads. No shadow-stop (audit-13 B7: the exists
 * rewrite resolves nested predicates through fresh scopes; over-demand is
 * duplicate-safe under EXISTS). Identity-deduped: ops repeat their
 * sources, so the walk sees the same lambda more than once.
 */
final class InnerDemand {

    private InnerDemand() {
    }

    /** The target-property HEADS the head's inner predicates read. */
    static Set<String> leaves(List<TypedSpec> ops, String head) {
        Set<String> out = new LinkedHashSet<>();
        for (TypedLambda lam : lambdas(ops, List.of(head))) {
            if (!lam.parameters().isEmpty()) {
                StoreResolver.collectParamPathHeads(lam,
                        lam.parameters().get(0), out);
            }
        }
        return out;
    }

    /** The inner lambdas (predicates over the head's target rows). */
    static List<TypedLambda> lambdas(List<TypedSpec> ops, List<String> path) {
        List<TypedLambda> found = new ArrayList<>();
        for (TypedSpec op : ops) {
            scanForLambdas(op, path, found);
        }
        IdentityHashMap<TypedLambda, Boolean> seen = new IdentityHashMap<>();
        List<TypedLambda> out = new ArrayList<>();
        for (TypedLambda lam : found) {
            if (seen.put(lam, Boolean.TRUE) == null) {
                out.add(lam);
            }
        }
        return out;
    }

    private static void scanForLambdas(TypedSpec n, List<String> path,
            List<TypedLambda> out) {
        if (n instanceof TypedLambda lam && !lam.parameters().isEmpty()) {
            for (TypedSpec b : lam.body()) {
                collect(b, lam.parameters().get(0), path, out);
            }
        }
        for (TypedSpec ch : n.children()) {
            scanForLambdas(ch, path, out);
        }
    }

    private static void collect(TypedSpec n, String userVar,
            List<String> path, List<TypedLambda> out) {
        if (n instanceof TypedNativeCall c && !c.args().isEmpty()) {
            // unwrap ->filter(f) chains on the receiver: their lambdas
            // demand target leaves too (filter-wrapped emptiness)
            TypedSpec recv = c.args().get(0);
            List<TypedLambda> chainLams = new ArrayList<>();
            while (recv instanceof TypedFilter tf) {
                chainLams.add(tf.predicate());
                recv = tf.source();
            }
            List<String> p = Substitution.pathOf(recv, userVar);
            if (p != null && p.equals(path)) {
                if (c.args().size() == 2
                        && c.args().get(1) instanceof TypedLambda lam
                        && !lam.parameters().isEmpty()) {
                    out.add(lam);
                }
                out.addAll(chainLams);
            }
        }
        if (n instanceof TypedPropertyAccess pna) {
            // filtered navigation consumed as a VALUE: unwrap the
            // multiplicity wrappers filteredNavLeafRead unwraps, then the
            // filter chain (same recognizer — they must not drift)
            TypedSpec src = pna.source();
            while (src instanceof TypedNativeCall w && w.args().size() == 1
                    && (w.callee().qualifiedName().equals(
                            "meta::pure::functions::multiplicity::toOne")
                        || w.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::first")
                        || w.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::head"))) {
                src = w.args().get(0);
            }
            List<TypedLambda> navLams = new ArrayList<>();
            while (src instanceof TypedFilter tf) {
                navLams.add(tf.predicate());
                src = tf.source();
            }
            if (!navLams.isEmpty()) {
                List<String> np = Substitution.pathOf(src, userVar);
                if (np != null && np.equals(path)) {
                    out.addAll(navLams);
                }
            }
        }
        for (TypedSpec ch : n.children()) {
            collect(ch, userVar, path, out);
        }
    }

    /** task #78 scalar-subquery IN: find in/contains calls whose
     * COLLECTION argument is an object-space chain rooted at TypedGetAll
     * (a let-inlined class query — engine temp-table semantics); resolve
     * each via the caller's dispatcher into a single-column relation and
     * key it by the CALL NODE identity for the substitution arm. */
    static java.util.Map<com.legend.compiler.spec.typed.TypedSpec,
            Substitution.InQueryRead> inQueryReads(
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> ops,
            java.util.List<com.legend.compiler.spec.typed.TypedLambda> terminals,
            java.util.function.Function<com.legend.compiler.spec.typed.TypedSpec,
                    com.legend.compiler.spec.typed.TypedSpec> rawResolver) {
        java.util.List<com.legend.compiler.spec.typed.TypedSpec> roots =
                new java.util.ArrayList<>();
        for (com.legend.compiler.spec.typed.TypedSpec op : ops) {
            if (op instanceof com.legend.compiler.spec.typed.TypedFilter f) {
                roots.addAll(f.predicate().body());
            }
        }
        for (com.legend.compiler.spec.typed.TypedLambda fn : terminals) {
            roots.addAll(fn.body());
        }
        // a trailing ->distinct() (the NATIVE-CALL spelling at this stage)
        // rides OUTSIDE the resolved relation as a relation-level DISTINCT;
        // an unresolvable chain returns null and keeps its ordinary wall.
        java.util.function.Function<com.legend.compiler.spec.typed.TypedSpec,
                com.legend.compiler.spec.typed.TypedSpec> resolver = chain -> {
            try {
                if (chain instanceof com.legend.compiler.spec.typed
                        .TypedNativeCall dc
                        && dc.args().size() == 1
                        && com.legend.builtin.Pure.nativeNamed("distinct",
                                dc.callee().signatureKey())) {
                    com.legend.compiler.spec.typed.TypedSpec rel0 =
                            rawResolver.apply(dc.args().get(0));
                    return rel0 == null ? null
                            : new com.legend.compiler.spec.typed
                                    .TypedDistinct(rel0, java.util.List.of(),
                                    rel0.info());
                }
                return rawResolver.apply(chain);
            } catch (RuntimeException e) {
                return null;
            }
        };
        return inQueryReadsOver(roots, resolver);
    }

    private static java.util.Map<com.legend.compiler.spec.typed.TypedSpec,
            Substitution.InQueryRead> inQueryReadsOver(
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> roots,
            java.util.function.Function<com.legend.compiler.spec.typed.TypedSpec,
                    com.legend.compiler.spec.typed.TypedSpec> resolver) {
        java.util.Map<com.legend.compiler.spec.typed.TypedSpec,
                Substitution.InQueryRead> out = new java.util.IdentityHashMap<>();
        for (com.legend.compiler.spec.typed.TypedSpec r : roots) {
            collectInQuery(r, resolver, out);
        }
        return out;
    }

    private static void collectInQuery(
            com.legend.compiler.spec.typed.TypedSpec n,
            java.util.function.Function<com.legend.compiler.spec.typed.TypedSpec,
                    com.legend.compiler.spec.typed.TypedSpec> resolver,
            java.util.Map<com.legend.compiler.spec.typed.TypedSpec,
                    Substitution.InQueryRead> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.args().size() == 2) {
            String key = c.callee().signatureKey();
            boolean isIn = com.legend.builtin.Pure.nativeNamed("in", key);
            boolean isContains =
                    com.legend.builtin.Pure.nativeNamed("contains", key);
            if (isIn || isContains) {
                com.legend.compiler.spec.typed.TypedSpec coll =
                        isContains ? c.args().get(0) : c.args().get(1);
                if (rootsAtGetAll(coll)
                        && !(coll.info().type()
                                instanceof com.legend.compiler.element.type
                                        .Type.RelationType)) {
                    com.legend.compiler.spec.typed.TypedSpec rel =
                            resolver.apply(coll);
                    if (rel != null && rel.info().type()
                            instanceof com.legend.compiler.element.type
                                    .Type.RelationType rt
                            && rt.columns().size() == 1) {
                        out.put(c, new Substitution.InQueryRead(rel,
                                rt.columns().get(0).name()));
                    }
                }
            }
        }
        for (com.legend.compiler.spec.typed.TypedSpec ch : n.children()) {
            collectInQuery(ch, resolver, out);
        }
    }

    private static boolean rootsAtGetAll(
            com.legend.compiler.spec.typed.TypedSpec n) {
        com.legend.compiler.spec.typed.TypedSpec cur = n;
        while (cur != null) {
            if (cur instanceof com.legend.compiler.spec.typed.TypedGetAll) {
                return true;
            }
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> ch =
                    cur.children();
            cur = ch.isEmpty() ? null : ch.get(0);
        }
        return false;
    }
}
