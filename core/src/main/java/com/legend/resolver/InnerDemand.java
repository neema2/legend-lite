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
}
