// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * Demanded NAVIGATE-TARGET materialization — recursive, hop-agnostic:
 * a tail continuing through the target's own class-typed slot
 * materializes THAT slot's target too, threading the chain prefix and
 * the inherited {@link TemporalContext} per hop (the engine re-enters
 * findPropertyMapping per hop; prefixes compose mechanically). Returns
 * {@link NavMat}: the pipeline + slot prefixes + the recursive SubNav
 * tree the substitution walks.
 */
final class NavMaterializer {

    private final ClassSources sources;
    private final AssociationJoins assocs;
    private final SyntheticHeads synthetics;

    NavMaterializer(ClassSources sources, AssociationJoins assocs,
            SyntheticHeads synthetics) {
        this.sources = sources;
        this.assocs = assocs;
        this.synthetics = synthetics;
    }

    /**
     * A demanded navigate TARGET, materialized with the slot demand its
     * tail paths imply — RECURSIVELY: a tail continuing through the
     * target's own class-typed navigate slot materializes THAT slot's
     * target too (the engine re-enters findPropertyMapping per hop;
     * prefixes compose mechanically: b_ + c_ + pk).
     */
    /** A demanded navigate target's material: the pipeline + slot prefixes
     * (as {@link Pipelines.Materialized}) PLUS the recursive SUB-navigation
     * tree the substitution walks — prefixes composed relative to THIS
     * target's row at every depth (hop-agnostic by construction). */
    record NavMat(TypedSpec pipeline, Map<String, String> slotPrefixes,
                  Set<String> stripped,
                  Map<String, Substitution.SubNav> subNavs) {}

    NavMat navTargetMaterialized(TemporalFrame temporal, String mappingFqn,
            String targetClassFqn, List<List<String>> tails) {
        return navTargetMaterialized(temporal, mappingFqn, targetClassFqn, tails,
                null, TemporalContext.NONE);
    }

    /** {@code chainPrefix}: the dotted path of the HEAD this target hangs
     * off; {@code inheritedDates}: the PARENT hop's effective context —
     * propagation flows hop-to-hop through temporal classes (engine
     * getMilestoningContextForQualifiedProperty), not only from the root. */
    NavMat navTargetMaterialized(TemporalFrame temporal, String mappingFqn,
            String targetClassFqn, List<List<String>> tails,
            String chainPrefix, TemporalContext inherited) {
        ClassSource t = sources.get(mappingFqn, targetClassFqn);
        // TEMPORAL GATE (same discipline as the union lift): the nested
        // materialization does not yet thread per-hop milestoning context
        // (engine: one context object per cursor, explicit dates override
        // per hop, context clears after non-temporal hops) — nested slots
        // under a temporal root/target/table leak unfiltered versions.
        // Those paths keep their previous LOUD walls.
        // a target ~filter changes join semantics under nested reads
        // (engine hoists it into the outer WHERE — isolation rule): loud.
        // Milestoned SLOT TARGETS are filterable when the hop has a date
        // context (chain spec or propagated root — the golden filters
        // StockProductTable by the hop's date); without one they stay loud.
        TemporalContext hopCtx =
                temporal.contextAt(chainPrefix, targetClassFqn, inherited);
        if (StoreResolver.containsFilter(t.pipeline())) {
            Pipelines.Materialized raw = Pipelines.materialize(
                    t.pipeline(), Set.of(), targetClassFqn);
            return new NavMat(raw.pipeline(), raw.slotPrefixes(),
                    raw.stripped(), Map.of());
        }
        Set<String> tSlots = Pipelines.slotAliases(t.pipeline());
        var tNavSteps = Pipelines.navSteps(t.pipeline());
        Set<String> tDemand = new LinkedHashSet<>();
        Set<String> tNavs = new LinkedHashSet<>();
        Map<String, List<List<String>>> subTails =
                new LinkedHashMap<>();
        Map<String, Set<String>> assocSubLeaves = new LinkedHashMap<>();
        for (List<String> tail : tails) {
            if (tail.isEmpty()) {
                continue;
            }
            TypedSpec b = t.bindings().get(
                    SyntheticHeads.realHead(tail.get(0)));
            if (b == null) {
                // ASSOC-SUB (union V3): the tail continues through an
                // ASSOCIATION end on this target (head y is a nav slot, z
                // on plain Y realizes via the association route). One extra
                // hop only; deeper tails and context-less temporal targets
                // keep their loud walls.
                if (tail.size() == 2) {
                    // a SYNTHETIC (filter-lifted) sub-head resolves by its
                    // REAL property; associationJoin below parks the pred
                    // on the sub-target (#70 — testQualifierInLambdaDeep)
                    var subClsOpt = assocs.assocTargetClassOf(
                            targetClassFqn,
                            SyntheticHeads.realHead(tail.get(0)));
                    // a UNION-mapped assoc target needs per-member routed
                    // conditions (V4) — the plain predicate returns PARTIAL
                    // rows; stays loud until that rung is built
                    if (subClsOpt.isPresent()
                            && !StoreResolver.containsConcatenate(sources
                                    .get(mappingFqn, subClsOpt.get())
                                    .pipeline())) {
                        String subChain = chainPrefix == null ? tail.get(0)
                                : chainPrefix + "." + tail.get(0);
                        // temporal sub-target: liftable when its chain-keyed
                        // spec (explicit hop date) OR the propagated context
                        // can stamp it — the nav-slot sub gate's condition
                        if (temporal.temporalStrategy(subClsOpt.get()) == null
                                || temporal.spec(subChain) != null
                                || !temporal.contextAt(subChain,
                                        subClsOpt.get(), hopCtx).isEmpty()) {
                            assocSubLeaves.computeIfAbsent(tail.get(0),
                                    k -> new LinkedHashSet<>()).add(tail.get(1));
                        }
                    }
                }
                continue;
            }
            StoreResolver.collectAliasReads(b, t.rowVar(), tSlots, tDemand);
            if (tail.size() >= 2) {
                // a CORRELATED pred on a filtered sub-hop cannot park
                // in-target — leave the step undemanded (loud read),
                // never an unfiltered join (wrong rows)
                if (synthetics.correlatedPred(tail.get(0)) != null) {
                    continue;
                }
                String subAlias = StoreResolver.navSlotAlias(b, t.rowVar(), tNavSteps.keySet());
                if (subAlias != null) {
                    // audit 12 F2: a TEMPORAL (or gated) sub-target must NOT
                    // materialize unfiltered under a non-temporal parent —
                    // the recursion's own gate returns a raw pipeline but
                    // cannot stop THIS level's join. Leave the sub-step
                    // undemanded: the leaf read stays LOUD downstream.
                    String subCls = ((TypedGetAll)
                            tNavSteps.get(subAlias).target()).classFqn();
                    ClassSource subT = sources.get(mappingFqn, subCls);
                    // TEMPORAL sub-target: liftable when its CHAIN-KEYED
                    // spec (explicit hop date) or propagated context can
                    // filter it (temporalTargetPipe in the resolver lambda
                    // below); no chain prefix or no context = stays loud.
                    boolean temporalSub = temporal.temporalStrategy(subCls) != null;
                    if (temporalSub && (chainPrefix == null
                            // a chain-keyed SPEC of any form (point, range
                            // sweep) is a usable context — temporalTargetPipe
                            // in the resolver lambda handles each; only the
                            // spec-less no-propagation case stays loud
                            || (temporal.spec(
                                    chainPrefix + "." + tail.get(0)) == null
                                && temporal.contextAt(chainPrefix + "." + tail.get(0),
                                    subCls, hopCtx).isEmpty())
                            // SNAPSHOT sub-unions stay loud: the engine
                            // mints a join PER dated-QP CALL SITE there
                            // (filter+project occurrences fan separately —
                            // expected 16 = our merged 8 x 2); from/thru
                            // sub-unions merge (partiallyMilestoning golden
                            // passes with the shared join). Per-call join
                            // identity is its own rung.
                            || (StoreResolver.containsConcatenate(subT.pipeline())
                                    && temporal.hasSnapshotScan(subT.pipeline())))) {
                        continue;
                    }
                    // milestoned SLOT TARGETS inside the sub's own pipeline
                    // are filterable when the SUB hop has a date context —
                    // the recursion's own slotDates/filterMilestonedJoin-
                    // Targets pass stamps them (audit 14 ungate: the
                    // blanket gate predated per-hop context threading);
                    // context-less they'd fan versions out — stays loud
                    boolean subHasContext = chainPrefix != null
                            && (temporal.spec(
                                    chainPrefix + "." + tail.get(0)) != null
                                || !temporal.contextAt(
                                    chainPrefix + "." + tail.get(0),
                                    subCls, hopCtx).isEmpty());
                    if ((temporal.hasMilestonedSlotTarget(subT.pipeline())
                                    && !subHasContext)
                            || StoreResolver.containsFilter(subT.pipeline())) {
                        continue;
                    }
                    tNavs.add(subAlias);
                    subTails.computeIfAbsent(subAlias, k -> new ArrayList<>())
                            .add(tail.subList(1, tail.size()));
                }
            }
        }
        tDemand = Pipelines.closeOverConditions(t.pipeline(), tDemand);
        final TemporalContext slotCtx = hopCtx;
        final Map<String, String> midByAlias = new LinkedHashMap<>();
        // SECOND head identities on one physical sub-slot (the 2a-x rule
        // at sub depth): the slot materializes once for the FIRST
        // identity; every other identity emits its OWN prefixed join
        // from the same nav step, with its own parked pred.
        Map<String, String> extraSubHeads = new LinkedHashMap<>();
        Map<String, List<List<String>>> extraSubTails = new LinkedHashMap<>();
        for (List<String> tail : tails) {
            if (tail.size() >= 2) {
                TypedSpec b2 = t.bindings().get(
                        SyntheticHeads.realHead(tail.get(0)));
                String a2 = b2 == null ? null
                        : StoreResolver.navSlotAlias(b2, t.rowVar(), tNavSteps.keySet());
                if (a2 != null) {
                    midByAlias.putIfAbsent(a2, tail.get(0));
                    if (!midByAlias.get(a2).equals(tail.get(0))
                            && synthetics.correlatedPred(tail.get(0)) == null) {
                        extraSubHeads.putIfAbsent(tail.get(0), a2);
                        extraSubTails.computeIfAbsent(tail.get(0),
                                        k -> new ArrayList<>())
                                .add(tail.subList(1, tail.size()));
                    }
                }
            }
        }
        final Map<String, NavMat> subMats = new LinkedHashMap<>();
        final Map<String, String> subClsByAlias = new LinkedHashMap<>();
        Pipelines.Materialized matM = Pipelines.materialize(
                t.pipeline(), tDemand, tNavs,
                targetClassFqn, (alias, cls) -> {
                    NavMat subMat = navTargetMaterialized(temporal, mappingFqn, cls,
                            subTails.getOrDefault(alias, List.of()),
                            chainPrefix == null ? null
                                    : chainPrefix + "." + midByAlias.get(alias),
                            hopCtx);
                    subMats.put(alias, subMat);
                    subClsByAlias.put(alias, cls);
                    TypedSpec sub = subMat.pipeline();
                    // a filter-LIFTED sub-hop's parked pred applies to the
                    // sub-target pipeline (engine golden
                    // testQualifierInLambdaDeep: the filtered subselect
                    // joins the chain; correlated heads never demand here)
                    String synthProp = midByAlias.get(alias);
                    if (synthProp != null) {
                        final NavMat sm2 = subMat;
                        sub = synthetics.applyToPipe(synthProp, sub,
                                (pp, pred) -> StoreResolver.predFilteredPipe(
                                        pp, sources.get(mappingFqn, cls),
                                        sm2.slotPrefixes(), pred, mappingFqn));
                    }
                    // per-hop temporal filter: the sub-hop's chain-keyed
                    // spec or propagated context (parent = THIS target)
                    if (temporal.temporalStrategy(cls) != null && chainPrefix != null) {
                        String subChain = chainPrefix + "." + midByAlias.get(alias);
                        TemporalFrame.TemporalSpec subSpec = temporal.spec(subChain);
                        if (subSpec != null) {
                            sub = temporal.temporalTargetPipe(t, sources.get(mappingFqn, cls),
                                    subChain, sub);
                        } else {
                            // DIMENSION-PROJECTED inheritance through a
                            // TEMPORAL parent (contextAt clears through
                            // non-temporal hops structurally — audit 13
                            // F4/F5), stamped by the sub CLASS's own
                            // temporality (bitemp pair / point / range)
                            sub = temporal.stampForClass(sub,
                                    temporal.contextAt(subChain, cls, hopCtx), cls);
                        }
                    }
                    return sub;
                });
        Map<String, Substitution.SubNav> subTree = new LinkedHashMap<>();
        for (var sm : subMats.entrySet()) {
            String prop = midByAlias.get(sm.getKey());
            String p = matM.slotPrefixes().get(sm.getKey());
            if (prop == null || p == null) {
                continue;
            }
            ClassSource subCs = sources.get(mappingFqn,
                    subClsByAlias.get(sm.getKey()));
            subTree.put(prop, new Substitution.SubNav(p, subCs.rowVar(),
                    subCs.bindings(),
                    composeSubNavPrefixes(p, sm.getValue().subNavs())));
        }
        TypedSpec pipe =
                !slotCtx.isEmpty() && temporal.hasMilestonedSlotTarget(t.pipeline())
                // milestoned SLOT-TARGET aliases filter by the hop context —
                // per each table's OWN dimension (cross-dimension takes
                // nothing; audit 13's own-dimension rule, now structural)
                ? temporal.filterMilestonedJoinTargets(matM.pipeline(), slotCtx)
                : matM.pipeline();
        pipe = foldAssocSubs(temporal, t, pipe, subTree, assocSubLeaves,
                chainPrefix);
        pipe = foldExtraSubIdentities(temporal, mappingFqn, t, pipe, subTree,
                extraSubHeads, extraSubTails, tNavSteps, chainPrefix, hopCtx);
        return new NavMat(pipe, matM.slotPrefixes(), matM.stripped(), subTree);
    }

    /** ASSOC-SUB folds (union V3): each collected end joins its target
     * INSIDE the materialized pipeline (the same descriptor->emission the
     * root uses) and rides the SubNav tree — the composed prefix (y_ + z_)
     * resolves the leaf on the joined row. */
    private TypedSpec foldAssocSubs(TemporalFrame temporal, ClassSource t,
            TypedSpec pipe, Map<String, Substitution.SubNav> subTree,
            Map<String, Set<String>> assocSubLeaves, String chainPrefix) {
        for (var e : assocSubLeaves.entrySet()) {
            String prop = e.getKey();
            String subChain = chainPrefix == null ? prop
                    : chainPrefix + "." + prop;
            AssociationJoins.AssocJoin aj = assocs.associationJoin(temporal,
                    t, prop, StoreResolver.Context.NONE, false,
                    e.getValue(), subChain);
            var leftRow = (com.legend.compiler.element.type.Type.RelationType)
                    pipe.info().type();
            List<com.legend.compiler.element.type.Type.Column> cols =
                    new ArrayList<>(leftRow.columns());
            for (var c : aj.targetRow().columns()) {
                cols.add(new com.legend.compiler.element.type.Type.Column(
                        aj.prefix() + c.name(), c.type(), c.multiplicity()));
            }
            pipe = new com.legend.compiler.spec.typed.TypedJoin(pipe,
                    aj.targetPipeline(), StoreResolver.leftKind(),
                    aj.condition(), java.util.Optional.of(aj.prefix()),
                    new com.legend.compiler.element.type.ExprType(
                            new com.legend.compiler.element.type.Type
                                    .RelationType(cols),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE));
            subTree.put(prop, new Substitution.SubNav(aj.prefix(),
                    aj.target().rowVar(), aj.target().bindings(), Map.of()));
        }
        return pipe;
    }

    /** EXTRA sub-slot identity joins (per-identity emission): the nav
     * step's own predicate joins the freshly-materialized sub target
     * (that identity's pred applied in-target) onto the pipeline. */
    private TypedSpec foldExtraSubIdentities(TemporalFrame temporal,
            String mappingFqn, ClassSource t, TypedSpec pipe,
            Map<String, Substitution.SubNav> subTree,
            Map<String, String> extraSubHeads,
            Map<String, List<List<String>>> extraSubTails,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> tNavSteps,
            String chainPrefix, TemporalContext hopCtx) {
        for (var e : extraSubHeads.entrySet()) {
            String prop = e.getKey();
            String alias = e.getValue();
            var step = tNavSteps.get(alias);
            if (!(step.target() instanceof TypedGetAll xg)) {
                continue;
            }
            String subChain = chainPrefix == null ? prop
                    : chainPrefix + "." + prop;
            NavMat xMat = navTargetMaterialized(temporal, mappingFqn,
                    xg.classFqn(),
                    extraSubTails.getOrDefault(prop, List.of()),
                    subChain, hopCtx);
            final NavMat xm2 = xMat;
            ClassSource xCs = sources.get(mappingFqn, xg.classFqn());
            TypedSpec xPipe = synthetics.applyToPipe(prop, xMat.pipeline(),
                    (pp, pred) -> StoreResolver.predFilteredPipe(
                            pp, xCs, xm2.slotPrefixes(), pred, mappingFqn));
            // the synthetic identity's own suffix keys the join prefix
            // (synonyms#f1 -> alias_f1_) — deterministic, collision-free
            // per identity by construction
            String xPrefix = alias + "_"
                    + (prop.indexOf('#') >= 0
                            ? prop.substring(prop.indexOf('#') + 1)
                            : "x") + "_";
            var xLeftRow = (com.legend.compiler.element.type.Type.RelationType)
                    pipe.info().type();
            var xRow = (com.legend.compiler.element.type.Type.RelationType)
                    xPipe.info().type();
            List<com.legend.compiler.element.type.Type.Column> xCols =
                    new ArrayList<>(xLeftRow.columns());
            for (var c : xRow.columns()) {
                xCols.add(new com.legend.compiler.element.type.Type.Column(
                        xPrefix + c.name(), c.type(), c.multiplicity()));
            }
            pipe = new com.legend.compiler.spec.typed.TypedJoin(pipe,
                    xPipe, StoreResolver.leftKind(),
                    step.predicate(), java.util.Optional.of(xPrefix),
                    new com.legend.compiler.element.type.ExprType(
                            new com.legend.compiler.element.type.Type
                                    .RelationType(xCols),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE));
            subTree.put(prop, new Substitution.SubNav(xPrefix,
                    xCs.rowVar(), xCs.bindings(),
                    composeSubNavPrefixes(xPrefix, xMat.subNavs())));
        }
        return pipe;
    }


    /** Re-root a child's SUB-navigation tree onto the parent row: every
     * prefix (relative to the child's row) gains the child's own join
     * prefix, recursively — composition is mechanical (b_ + c_ + pk). */
    private static Map<String, Substitution.SubNav> composeSubNavPrefixes(
            String p, Map<String, Substitution.SubNav> kids) {
        if (kids.isEmpty()) {
            return kids;
        }
        Map<String, Substitution.SubNav> out = new LinkedHashMap<>();
        for (var e : kids.entrySet()) {
            Substitution.SubNav k = e.getValue();
            out.put(e.getKey(), new Substitution.SubNav(p + k.prefix(),
                    k.rowVar(), k.bindings(),
                    composeSubNavPrefixes(p, k.children())));
        }
        return out;
    }
}
