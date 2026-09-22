// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
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

    /** §4AD batch 7 census (design §8 step a): classify every FILTER-
     * predicate to-many navigation consumption — a genuine EMPTINESS
     * call (isEmpty/isNotEmpty/exists/forAll: row-count-preserving
     * semantics by definition, stays a semi-join) vs a PREDICATE-READ
     * (boolean leaves over the exploded values — the dedup leg's
     * observable-change set, charter decision 2). Measurement only. */
    static void existsKindScan(com.legend.compiler.spec.typed.TypedSpec n,
            String userVar, ClassSource cs,
            java.util.function.BiPredicate<ClassSource, String> toManyHead,
            boolean underEmptiness) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && !nc.args().isEmpty()) {
            String key = nc.callee().signatureKey();
            if (com.legend.builtin.Pure.nativeNamed("isEmpty", key)
                    || com.legend.builtin.Pure.nativeNamed("isNotEmpty", key)
                    || com.legend.builtin.Pure.nativeNamed("exists", key)
                    || com.legend.builtin.Pure.nativeNamed("forAll", key)) {
                existsKindScan(nc.args().get(0), userVar, cs, toManyHead,
                        true);
                for (int i = 1; i < nc.args().size(); i++) {
                    existsKindScan(nc.args().get(i), userVar, cs,
                            toManyHead, false);
                }
                return;
            }
        }
        java.util.List<String> p = Substitution.pathOf(n, userVar);
        if (p != null && !p.isEmpty() && toManyHead.test(cs, p.get(0))) {
            return;
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedLambda l
                && l.parameters().contains(userVar)) {
            return;
        }
        for (com.legend.compiler.spec.typed.TypedSpec c : n.children()) {
            existsKindScan(c, userVar, cs, toManyHead, underEmptiness);
        }
    }

    /** GRAPH-terminal demand: tree LEAF paths feed slot demand (a leaf's
     * binding may read a demanded join slot); an EMBEDDED/INLINE ctor
     * child reads the PARENT row, so its demanded sub-properties ride as
     * 2-hop paths (Inline splices with @Join columns). Class-typed
     * children correlate — materialized by buildGraphNode, not here. */
    static void treeDemandPaths(
            java.util.List<com.legend.compiler.spec.typed.TypedGraphTree> tree,
            ClassSource cs,
            com.legend.compiler.element.ModelContext ctx,
            java.util.Set<java.util.List<String>> projectionPaths) {
        for (com.legend.compiler.spec.typed.TypedGraphTree node : tree) {
            if (node.children().isEmpty()) {
                if (!manyPrimSlotLeaf(cs, ctx, node.property())) {
                    projectionPaths.add(java.util.List.of(node.property()));
                }
            } else if (Substitution.embeddedPartialOf(cs.bindings()
                    .get(node.property())) != null) {
                for (com.legend.compiler.spec.typed.TypedGraphTree sub
                        : node.children()) {
                    projectionPaths.add(java.util.List.of(
                            node.property(), sub.property()));
                }
            }
        }
    }

    /** A TO-MANY PRIMITIVE leaf reading a column THROUGH a join slot
     * ({@code $row.<slot>.OTHER_NAME}, declared {@code String[*]}): it
     * takes the CORRELATED per-root aggregation arm — demanding the slot
     * would FAN the base out one row per value (engine: otherNames
     * serializes {@code ["abc","def"]} per person). */
    private static boolean manyPrimSlotLeaf(ClassSource cs,
            com.legend.compiler.element.ModelContext ctx, String prop) {
        TypedSpec b0 = cs.bindings().get(prop);
        var dp0 = ctx.findProperty(cs.classFqn(), prop).orElse(null);
        if (b0 == null || dp0 == null
                || dp0.type() instanceof com.legend.compiler.element.type
                        .Type.ClassType
                || (dp0.multiplicity() instanceof com.legend.compiler.element
                        .type.Multiplicity.Bounded mb0
                        && Integer.valueOf(1).equals(mb0.upper()))) {
            return false;
        }
        TypedSpec bb = b0;
        if (bb instanceof TypedNativeCall w && w.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())) {
            bb = w.args().get(0);
        }
        return bb instanceof TypedPropertyAccess pa1
                && pa1.source() instanceof TypedPropertyAccess mid
                && mid.source()
                        instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(cs.rowVar())
                && (Pipelines.navSteps(cs.pipeline())
                                .containsKey(mid.property())
                        || Pipelines.slotAliases(cs.pipeline())
                                .contains(mid.property()));
    }

    private InnerDemand() {
    }

    /** NAV-DATE demand (#32): temporal spec dates that READ A NAVIGATION
     * off the parent row ({@code $o.orderDetails.settlementDate}) demand
     * that chain like any other read — the composed date column must
     * materialize for the outer-date calculus to window against it. */
    static Set<List<String>> navDatePaths(
            java.util.Collection<TemporalFrame.TemporalSpec> specs) {
        Set<List<String>> out = new LinkedHashSet<>();
        for (TemporalFrame.TemporalSpec sp : specs) {
            for (TypedSpec dexp : sp.dates()) {
                List<String> path = TemporalFrame.singleVarChain(dexp);
                if (path != null && path.size() >= 2) {
                    out.add(path);
                }
            }
        }
        return out;
    }

    /** {@code paths} with the NAV-DATE chains PREPENDED (registration
     * order matters: the date chain must precede its consuming head). */
    static Set<List<String>> withNavDatePaths(Set<List<String>> paths,
            java.util.Collection<TemporalFrame.TemporalSpec> specs) {
        Set<List<String>> datePaths = navDatePaths(specs);
        if (datePaths.isEmpty()) {
            return paths;
        }
        Set<List<String>> merged = new LinkedHashSet<>(datePaths);
        merged.addAll(paths);
        return merged;
    }

    /** The query's read-path heads in FIRST-READ order: the engine adds the
     * terminal's joins first (keys, aggregates, projection columns left to
     * right) and the filter's after — a filter over a column the terminal
     * also reads rides that join (testGroupByWithTwoOpenVariablesInAggAndFilter). */
    static List<String> firstReadHeads(Set<List<String>> projectionPaths,
            Set<List<String>> filterPaths) {
        List<String> heads = new java.util.ArrayList<>();
        for (Set<List<String>> group : List.of(projectionPaths, filterPaths)) {
            for (List<String> p : group) {
                if (!p.isEmpty() && !heads.contains(p.get(0))) {
                    heads.add(p.get(0));
                }
            }
        }
        return heads;
    }

    /** The root's step aliases in FIRST-READ order ({@link SlotOrder}):
     * {@code heads} are the query's read-path heads in first-read order
     * (filter paths, then the terminal's columns); each head names the
     * navigate steps registered under it ({@code navHeadByAlias}) and
     * the same-named slot. */
    static List<String> stepOrder(List<String> heads,
            java.util.Map<String, String> navHeadByAlias) {
        List<String> out = new java.util.ArrayList<>();
        for (String head : heads) {
            for (String alias : aliasesOf(head, navHeadByAlias)) {
                if (!out.contains(alias)) {
                    out.add(alias);
                }
            }
        }
        return out;
    }

    /** The sink's invariant as a dependency ({@link SlotOrder}): a
     * milestoned head whose spec date READS A NAVIGATION (a nav-date
     * chain) must sit ABOVE that chain's steps — consumer alias → the
     * nav-date aliases it follows. */
    static java.util.Map<String, Set<String>> navDateConsumers(
            java.util.Map<String, TemporalFrame.TemporalSpec> chainSpecs,
            java.util.Map<String, String> navHeadByAlias) {
        java.util.Map<String, Set<String>> out = new java.util.LinkedHashMap<>();
        for (var e : chainSpecs.entrySet()) {
            for (TypedSpec dexp : e.getValue().dates()) {
                List<String> path = TemporalFrame.singleVarChain(dexp);
                if (path == null || path.size() < 2) {
                    continue;
                }
                Set<String> dates = aliasesOf(path.get(0), navHeadByAlias);
                for (String consumer : aliasesOf(e.getKey(), navHeadByAlias)) {
                    out.computeIfAbsent(consumer, k -> new LinkedHashSet<>()).addAll(dates);
                }
            }
        }
        return out;
    }

    private static Set<String> aliasesOf(String head,
            java.util.Map<String, String> navHeadByAlias) {
        Set<String> out = new LinkedHashSet<>();
        out.add(head);
        for (var e : navHeadByAlias.entrySet()) {
            if (e.getValue().equals(head)) {
                out.add(e.getKey());
            }
        }
        return out;
    }

    /** The nav-step aliases carrying NAV-DATE chains — these steps SINK
     * below every consuming head join ({@link Pipelines#sinkNavSteps}):
     * the composed date column must sit on the head's LEFT row. */
    static Set<String> navDateAliases(
            java.util.Collection<TemporalFrame.TemporalSpec> specs,
            java.util.Map<String, String> navHeadByAlias) {
        Set<String> heads = new LinkedHashSet<>();
        for (List<String> dp : navDatePaths(specs)) {
            heads.add(dp.get(0));
        }
        Set<String> aliases = new LinkedHashSet<>();
        for (var e : navHeadByAlias.entrySet()) {
            if (heads.contains(e.getValue())) {
                aliases.add(e.getKey());
            }
        }
        return aliases;
    }

    /** The target-property HEADS the head's inner predicates read. */
    static Set<String> leaves(List<TypedSpec> ops, String head) {
        Set<String> out = new LinkedHashSet<>();
        for (TypedLambda lam : lambdas(ops, List.of(head))) {
            if (!lam.parameters().isEmpty()) {
                collectParamPathHeads(lam,
                        lam.parameters().get(0), out);
            }
        }
        return out;
    }

    /** Nav-step demand for an ExistsSub target (#69/#70): pred paths
     * (correlated + closed parked — the Fork family) and CONTINUED leaf
     * chains all demand the target's OWN class-typed navigate steps; the
     * materialization joins them and the SubNav dispatch reads through
     * the prefixes. Fills {@code aliasOut} (prop &rarr; alias), returns
     * the demanded alias set. Identity dedup keeps join count
     * engine-equal. */
    static Set<String> navStepDemand(ClassSource t, Set<String> navStepKeys,
            @com.legend.base.Nullable TypedLambda corrPred, List<TypedLambda> parkedPreds,
            Set<List<String>> chains, java.util.Map<String, String> aliasOut) {
        Set<String> demand = new LinkedHashSet<>();
        Set<List<String>> paths = new LinkedHashSet<>();
        if (corrPred != null) {
            for (TypedSpec b : corrPred.body()) {
                FlattenOps.consumedPaths(b, corrPred.parameters().get(0),
                        paths);
            }
        }
        for (TypedLambda cp : parkedPreds) {
            for (TypedSpec b : cp.body()) {
                FlattenOps.consumedPaths(b, cp.parameters().get(0), paths);
            }
        }
        for (List<String> pp : paths) {
            if (pp.size() >= 2) {
                demandStep(t, navStepKeys, pp.get(0), demand, aliasOut);
            }
        }
        for (List<String> lc : chains) {
            demandStep(t, navStepKeys, lc.get(0), demand, aliasOut);
        }
        return demand;
    }

    /** The nav-step ALIAS a binding reads (a bare class-typed slot read,
     * toOne-wrapped or not), null otherwise. */
    static @com.legend.base.Nullable String navSlotAlias(
            @com.legend.base.Nullable TypedSpec binding, String rowVar,
                                       Set<String> navAliases) {
        TypedSpec inner = binding;
        if (inner instanceof TypedNativeCall c
                && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
            inner = c.args().get(0);
        }
        if (inner instanceof TypedPropertyAccess pa
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(rowVar)
                && navAliases.contains(pa.property())) {
            return pa.property();
        }
        return null;
    }

    private static void demandStep(ClassSource t, Set<String> navStepKeys,
            String prop, Set<String> demand,
            java.util.Map<String, String> aliasOut) {
        TypedSpec hb = t.bindings().get(prop);
        String al = hb == null ? null
                : navSlotAlias(hb, t.rowVar(), navStepKeys);
        if (al != null) {
            demand.add(al);
            aliasOut.put(prop, al);
        }
    }

    /** CONTINUED leaf chains past a value-position filtered navigation:
     * a read {@code <head>->filter(..)->toOne().p1...pn} yields
     * {@code [p1..pn]} — the class hops the ExistsSub target must
     * materialize (nav-step demand) so the scalar leaf projects through
     * the joined row (the orgByName('X').parent.name family). */
    static Set<List<String>> leafChains(List<TypedSpec> ops, String head) {
        Set<List<String>> out = new LinkedHashSet<>();
        for (TypedSpec op : ops) {
            scanForChains(op, head, out);
        }
        return out;
    }

    private static void scanForChains(TypedSpec n, String head,
            Set<List<String>> out) {
        if (n instanceof TypedLambda lam && !lam.parameters().isEmpty()) {
            for (TypedSpec b : lam.body()) {
                collectChains(b, lam.parameters().get(0), head, out);
            }
        }
        for (TypedSpec ch : n.children()) {
            scanForChains(ch, head, out);
        }
    }

    private static void collectChains(TypedSpec n, String userVar,
            String head, Set<List<String>> out) {
        if (n instanceof TypedPropertyAccess) {
            java.util.LinkedList<String> chain = new java.util.LinkedList<>();
            TypedSpec src = n;
            while (src instanceof TypedPropertyAccess p) {
                chain.addFirst(p.property());
                src = p.source();
            }
            while (src instanceof TypedNativeCall w && w.args().size() == 1
                    && (com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())
                        || w.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::first")
                        || w.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::head"))) {
                src = w.args().get(0);
            }
            boolean sawFilter = false;
            while (src instanceof TypedFilter tf) {
                sawFilter = true;
                src = tf.source();
            }
            if (sawFilter && chain.size() >= 2) {
                List<String> hp = Substitution.pathOf(src, userVar);
                if (hp != null && hp.size() == 1 && hp.get(0).equals(head)) {
                    out.add(List.copyOf(chain));
                }
            }
        }
        for (TypedSpec ch : n.children()) {
            collectChains(ch, userVar, head, out);
        }
    }

    /** Multi-hop paths consumed under an emptiness-family call — the
     * class-typed-leaf EXISTS registration keys off these. A path found
     * INSIDE another emptiness call's PREDICATE lands in {@code nested}
     * (the engine processes it in the enclosing subselect's OWN scope —
     * the exploded-chain rung), a top-scope path in {@code direct} (the
     * per-fanned-row flat form: testIsEmptyNested's golden LEFT-JOIN +
     * semi-join key null check rides the fanned employee row). */
    static void collectEmptinessChainPaths(TypedSpec n, String userVar,
            Set<List<String>> direct, Set<List<String>> nested) {
        collectEmptinessChainPaths(n, userVar, false, direct, nested);
    }

    private static void collectEmptinessChainPaths(TypedSpec n,
            String userVar, boolean inPred, Set<List<String>> direct,
            Set<List<String>> nested) {
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;   // shadowing: the substitution stops here too
        }
        if (n instanceof TypedNativeCall c && !c.args().isEmpty()) {
            String key = c.callee().signatureKey();
            if (com.legend.builtin.Pure.nativeNamed("isEmpty", key)
                    || com.legend.builtin.Pure.nativeNamed("isNotEmpty", key)
                    || com.legend.builtin.Pure.nativeNamed("exists", key)) {
                // an INLINED derived CONCATENATION under the emptiness
                // call contributes each member's chain (the concat-split
                // emission consumes the dotted materials per branch)
                // the FILTER-WRAPPED spelling — isNotEmpty(filter(
                // $this.firm.employees, pred)) ≡ exists(nav, pred) — peels
                // to the same dotted chain (the substitution's filter-
                // wrapped emptiness arm merges the predicates)
                TypedSpec a0 = c.args().get(0);
                while (a0 instanceof TypedFilter tf0) {
                    // the peeled predicates are SUBSELECT scope
                    collectEmptinessChainPaths(tf0.predicate(), userVar,
                            true, direct, nested);
                    a0 = tf0.source();
                }
                List<TypedSpec> heads =
                        a0 instanceof TypedNativeCall cc
                        && "meta::pure::functions::collection::concatenate"
                                .equals(cc.callee().qualifiedName())
                        ? cc.args() : List.of(a0);
                for (TypedSpec h : heads) {
                    List<String> p = Substitution.pathOf(h, userVar);
                    if (p != null && p.size() >= 2) {
                        (inPred ? nested : direct).add(p);
                    }
                }
                collectEmptinessChainPaths(a0, userVar, inPred, direct,
                        nested);
                for (int i = 1; i < c.args().size(); i++) {
                    collectEmptinessChainPaths(c.args().get(i), userVar,
                            true, direct, nested);
                }
                return;
            }
        }
        for (TypedSpec c : n.children()) {
            collectEmptinessChainPaths(c, userVar, inPred, direct, nested);
        }
    }

    /** Heads of property paths over {@code param} in the lambda's body. */
    static void collectParamPathHeads(TypedSpec n, String param,
            Set<String> out) {
        List<String> p = Substitution.pathOf(n, param);
        if (p != null && !p.isEmpty()) {
            out.add(p.get(0));
        }
        for (TypedSpec ch : n.children()) {
            collectParamPathHeads(ch, param, out);
        }
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
        // project OVER THE INSTANCE ($x.<head>->toOne()->project(cols)):
        // the col lambdas read the head's target rows — inner lambdas
        // like exists predicates (nestedScope registries, leaf demand)
        if (n instanceof TypedProject tp) {
            List<String> pp = Substitution.pathOf(
                    instanceProjectSource(tp), userVar);
            if (pp != null && pp.equals(path)) {
                for (TypedFuncCol c : tp.columns()) {
                    out.add(c.fn());
                }
            }
        }
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
                    && (com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())
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

    /** The source of a project OVER AN INSTANCE, multiplicity wrappers
     * unwrapped — ONE recognizer for the demand scan and the
     * substitution arm (they must not drift). */
    static TypedSpec instanceProjectSource(TypedProject tp) {
        TypedSpec src = tp.source();
        while (src instanceof TypedNativeCall w && w.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())
                    || w.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::first")
                    || w.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::head"))) {
            src = w.args().get(0);
        }
        return src;
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
                    com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSpec> rawResolver) {
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
                com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSpec> resolver =
                chain -> {
            try {
                // RECURSIVE peel (ledger cluster 47): distinct arrives as
                // a native call (DistinctChecker's non-relation overload)
                // but take/limit ALWAYS emit TypedLimit (CoreFn owns both
                // overloads — the old native-call take arm was dead), and
                // peels must COMPOSE (->distinct()->take(n) both ways).
                return peelInChain(chain, rawResolver);
            } catch (com.legend.error.NotImplementedException
                    | com.legend.error.LegendCompileException e) {
                // EXPECTED walls only: this chain is not resolvable as an
                // in-query read — keep its ordinary wall. A broad
                // RuntimeException catch here degraded genuine resolver
                // BUGS (NPE/ISE) into "not an in-query read" and a
                // DIFFERENT, possibly wrong lowering (audit T §4.4) —
                // those now propagate loudly.
                return null;
            }
        };
        return inQueryReadsOver(roots, resolver);
    }

    /** Self-recursive in-chain peel — see the resolver lambda's comment. */
    private static com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSpec
            peelInChain(com.legend.compiler.spec.typed.TypedSpec chain,
            java.util.function.Function<com.legend.compiler.spec.typed.TypedSpec,
                    com.legend.compiler.spec.typed.@com.legend.base.Nullable
                            TypedSpec> rawResolver) {
        if (chain instanceof com.legend.compiler.spec.typed
                        .TypedNativeCall dc
                && dc.args().size() == 1
                && com.legend.builtin.Pure.nativeNamed("distinct",
                        dc.callee().signatureKey())) {
            var rel0 = peelInChain(dc.args().get(0), rawResolver);
            return rel0 == null || !com.legend.compiler.element.type.Type
                            .isRelation(rel0.info().type()) ? null
                    : new com.legend.compiler.spec.typed.TypedDistinct(
                            rel0, java.util.List.of(), rel0.info());
        }
        if (chain instanceof com.legend.compiler.spec.typed.TypedLimit tl) {
            var rel0 = peelInChain(tl.source(), rawResolver);
            return rel0 == null || !com.legend.compiler.element.type.Type
                            .isRelation(rel0.info().type()) ? null
                    : new com.legend.compiler.spec.typed.TypedLimit(
                            rel0, tl.count(), rel0.info());
        }
        return rawResolver.apply(chain);
    }

    /** The path scanner's shape: (node, userVar, out-path-set). */
    @FunctionalInterface
    interface PathScan {
        void scan(com.legend.compiler.spec.typed.TypedSpec n, String userVar,
                java.util.Set<java.util.List<String>> out);
    }

    /** AUTO-MAP with a MULTI-PATH body (map(chain, m|$m.first+' '+
     * $m.last) — an inlined derived leaf over a navigation chain):
     * pathOf flattens only single-path bodies, so compose the map
     * SOURCE's path with each body leaf path — the demand the
     * substitution's own walk consumes (one-funnel discipline, #78). */
    static void composeAutoMapPaths(com.legend.compiler.spec.typed.TypedSpec n,
            String userVar, java.util.Set<java.util.List<String>> out,
            PathScan scanner) {
        if (!(n instanceof com.legend.compiler.spec.typed.TypedMap am)
                || am.mapper().parameters().size() != 1) {
            return;
        }
        java.util.List<String> prefix =
                Substitution.pathOf(am.source(), userVar);
        if (prefix == null) {
            // a CAST in SOURCE position (`chain->subType(@Sub)` auto-mapped
            // over a [0..1] hop — testRoutingWithSubtypePropagation's
            // `…manager->subType(@PersonExtension).name` with the derived
            // leaf inlined): the body's reads are reads OFF THE CAST —
            // inline the element and let pathOf's cast arm qualify the
            // leaf (stc_<Sub>___<leaf>), exactly as the substitution's
            // own inlining reads it (one funnel, batch 107)
            if (am.source() instanceof com.legend.compiler.spec.typed.TypedNativeCall sc
                    && com.legend.builtin.NativeFn.SubtypeForm.of(sc.callee().qualifiedName()).orElse(null) == com.legend.builtin.NativeFn.SubtypeForm.SUB_TYPE
                    && !sc.args().isEmpty()
                    && Substitution.pathOf(sc.args().get(0), userVar) != null) {
                for (com.legend.compiler.spec.typed.TypedSpec mb : am.mapper().body()) {
                    scanner.scan(Substitution.inlineParam(mb,
                            am.mapper().parameters().get(0), am.source()),
                            userVar, out);
                }
            }
            return;
        }
        java.util.Set<java.util.List<String>> bodyPaths =
                new java.util.LinkedHashSet<>();
        for (com.legend.compiler.spec.typed.TypedSpec mb : am.mapper().body()) {
            scanner.scan(mb, am.mapper().parameters().get(0), bodyPaths);
        }
        for (java.util.List<String> bp : bodyPaths) {
            java.util.List<String> full = new java.util.ArrayList<>(prefix);
            full.addAll(bp);
            out.add(full);
        }
    }

    /** tdsContains fn lambdas bind the OUTER object: hand each lambda
     * body + its own param to the caller's scanner (the shadow stop in
     * both path walkers would otherwise drop them — task #78). */
    static void scanTdsContainsFns(com.legend.compiler.spec.typed.TypedSpec n,
            String userVar,
            java.util.function.BiConsumer<com.legend.compiler.spec.typed
                    .TypedSpec, String> each) {
        if (!(n instanceof com.legend.compiler.spec.typed.TypedNativeCall tdc)
                || !com.legend.builtin.Pure.nativeNamed("tdsContains",
                        tdc.callee().signatureKey())
                || tdc.args().size() < 2
                || !(tdc.args().get(0) instanceof
                        com.legend.compiler.spec.typed.TypedVariable ov)
                || !ov.name().equals(userVar)) {
            return;
        }
        com.legend.compiler.spec.typed.TypedSpec fns = tdc.args().get(1);
        java.util.List<com.legend.compiler.spec.typed.TypedSpec> fl =
                fns instanceof com.legend.compiler.spec.typed
                        .TypedCollection tcl
                ? tcl.elements() : java.util.List.of(fns);
        for (com.legend.compiler.spec.typed.TypedSpec f : fl) {
            if (f instanceof com.legend.compiler.spec.typed.TypedLambda lam2
                    && lam2.parameters().size() == 1) {
                for (com.legend.compiler.spec.typed.TypedSpec b
                        : lam2.body()) {
                    each.accept(b, lam2.parameters().get(0));
                }
            }
        }
    }

    private static java.util.Map<com.legend.compiler.spec.typed.TypedSpec,
            Substitution.InQueryRead> inQueryReadsOver(
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> roots,
            java.util.function.Function<com.legend.compiler.spec.typed.TypedSpec,
                    com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSpec> resolver) {
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
                    com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSpec> resolver,
            java.util.Map<com.legend.compiler.spec.typed.TypedSpec,
                    Substitution.InQueryRead> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall tc
                && com.legend.builtin.Pure.nativeNamed("tdsContains",
                        tc.callee().signatureKey())
                && tc.args().size() >= 3) {
            // tdsContains: the TDS arg is a relation CHAIN (project over
            // a class extent) — resolve it like the in-subquery colls;
            // the substitution arm pairs functions to columns (task #78)
            com.legend.compiler.spec.typed.TypedSpec tdsArg =
                    tc.args().get(tc.args().size() == 3 ? 2 : 3);
            com.legend.compiler.spec.typed.TypedSpec rel =
                    resolver.apply(tdsArg);
            if (rel != null && com.legend.compiler.element.type.Type
                    .isRelation(rel.info().type())) {
                out.put(tc, new Substitution.InQueryRead(rel, null));
            }
        }
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
                        && !com.legend.compiler.element.type.Type
                                .isRelation(coll.info().type())) {
                    com.legend.compiler.spec.typed.TypedSpec rel =
                            resolver.apply(coll);
                    if (rel != null
                            && com.legend.compiler.element.type.Type
                                    .relationSchema(rel.info().type())
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

    /** OCCURRENCE-SPLIT demand: a 3+-hop chain read in BOTH filter and
     * projection position — its snapshot sub-union step joins once per
     * occurrence class (engine per-call join identity, row-semantic). */
    static java.util.Set<String> occurrenceSplitChains(
            java.util.Set<java.util.List<String>> filterPaths,
            java.util.Set<java.util.List<String>> projectionPaths) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        for (java.util.List<String> fp : filterPaths) {
            if (fp.size() < 3) {
                continue;
            }
            for (java.util.List<String> pp : projectionPaths) {
                if (pp.size() >= 3 && pp.get(0).equals(fp.get(0))
                        && pp.get(1).equals(fp.get(1))) {
                    out.add(fp.get(0) + "." + fp.get(1));
                }
            }
        }
        return out;
    }


    /**
     * The FILTER-position scan: a bare to-many crossing consumed AS A
     * COLLECTION by contains/in is set MEMBERSHIP (EXISTS route — engine
     * testContainsOnToManyProperty golden) and demands only its HEAD's
     * exists material, never the explosion join; everything else records
     * bare demand exactly as FlattenOps.consumedPaths. (Moved from
     * StoreResolver at its file guardrail, batch 78.)
     */
    static void memberScan(TypedSpec n, String userVar, ClassSource cs,
            Set<List<String>> out,
            java.util.function.BiPredicate<ClassSource, String> isToManyAssocHead) {
        if (n instanceof TypedNativeCall mc
                && mc.args().size() == 2) {
            String key = mc.callee().signatureKey();
            boolean isContains = com.legend.builtin.Pure.nativeNamed("contains", key);
            boolean isIn = com.legend.builtin.Pure.nativeNamed("in", key);
            if (isContains || isIn) {
                TypedSpec coll = isContains ? mc.args().get(0) : mc.args().get(1);
                TypedSpec other = isContains ? mc.args().get(1) : mc.args().get(0);
                List<String> cp = coll
                        instanceof TypedPropertyAccess
                        ? Substitution.pathOf(coll, userVar) : null;
                if (cp != null && cp.size() == 2 && isToManyAssocHead.test(cs, cp.get(0))) {
                    out.add(List.of(cp.get(0)));
                    memberScan(other, userVar, cs, out, isToManyAssocHead);
                    return;
                }
            }
        }
        scanTdsContainsFns(n, userVar,
                (b, pv) -> memberScan(b, pv, cs, out, isToManyAssocHead));
        // FILTER-POSITION to-many aggregate (audit 9's join-explosion
        // hazard): the node routes through the AGG DEMAND SCAN (the same
        // parent-copy grouped-subselect machinery as projection position —
        // aggregates are single-row, so the joined column compares safely
        // in WHERE). memberScan SKIPS it (its nav path must not become an
        // implicit EXISTS); a shape the agg scan fails to register still
        // dies loud at the Substitution backstop ("the aggregate demand
        // scan did not recognize this shape").
        if (n instanceof TypedNativeCall ac
                && !ac.args().isEmpty()
                && CorrelatedSubselects.isAggregate(ac)
                && CorrelatedSubselects.containsToManyCrossing(
                        ac.args().get(0), userVar, cs,
                        isToManyAssocHead)) {
            return;
        }
        List<String> path = Substitution.pathOf(n, userVar);
        if (path != null) {
            out.add(path);
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;
        }
        for (TypedSpec c : n.children()) {
            memberScan(c, userVar, cs, out, isToManyAssocHead);
        }
    }
}
