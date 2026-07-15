// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
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

    NavMaterializer(ClassSources sources) {
        this.sources = sources;
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
            String targetClassFqn, java.util.List<java.util.List<String>> tails) {
        return navTargetMaterialized(temporal, mappingFqn, targetClassFqn, tails,
                null, TemporalContext.NONE);
    }

    /** {@code chainPrefix}: the dotted path of the HEAD this target hangs
     * off; {@code inheritedDates}: the PARENT hop's effective context —
     * propagation flows hop-to-hop through temporal classes (engine
     * getMilestoningContextForQualifiedProperty), not only from the root. */
    NavMat navTargetMaterialized(TemporalFrame temporal, String mappingFqn,
            String targetClassFqn, java.util.List<java.util.List<String>> tails,
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
        Set<String> tDemand = new java.util.LinkedHashSet<>();
        Set<String> tNavs = new java.util.LinkedHashSet<>();
        Map<String, java.util.List<java.util.List<String>>> subTails =
                new java.util.LinkedHashMap<>();
        for (java.util.List<String> tail : tails) {
            if (tail.isEmpty()) {
                continue;
            }
            TypedSpec b = t.bindings().get(tail.get(0));
            if (b == null) {
                continue;
            }
            StoreResolver.collectAliasReads(b, t.rowVar(), tSlots, tDemand);
            if (tail.size() >= 2) {
                String subAlias = StoreResolver.navSlotAlias(b, t.rowVar(), tNavSteps.keySet());
                if (subAlias != null) {
                    // audit 12 F2: a TEMPORAL (or gated) sub-target must NOT
                    // materialize unfiltered under a non-temporal parent —
                    // the recursion's own gate returns a raw pipeline but
                    // cannot stop THIS level's join. Leave the sub-step
                    // undemanded: the leaf read stays LOUD downstream.
                    String subCls = ((com.legend.compiler.spec.typed.TypedGetAll)
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
        final Map<String, String> midByAlias = new java.util.LinkedHashMap<>();
        for (java.util.List<String> tail : tails) {
            if (tail.size() >= 2) {
                TypedSpec b2 = t.bindings().get(tail.get(0));
                String a2 = b2 == null ? null
                        : StoreResolver.navSlotAlias(b2, t.rowVar(), tNavSteps.keySet());
                if (a2 != null) {
                    midByAlias.putIfAbsent(a2, tail.get(0));
                }
            }
        }
        final Map<String, NavMat> subMats = new java.util.LinkedHashMap<>();
        final Map<String, String> subClsByAlias = new java.util.LinkedHashMap<>();
        Pipelines.Materialized matM = Pipelines.materialize(
                t.pipeline(), tDemand, tNavs,
                targetClassFqn, (alias, cls) -> {
                    NavMat subMat = navTargetMaterialized(temporal, mappingFqn, cls,
                            subTails.getOrDefault(alias, java.util.List.of()),
                            chainPrefix == null ? null
                                    : chainPrefix + "." + midByAlias.get(alias),
                            hopCtx);
                    subMats.put(alias, subMat);
                    subClsByAlias.put(alias, cls);
                    TypedSpec sub = subMat.pipeline();
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
        Map<String, Substitution.SubNav> subTree = new java.util.LinkedHashMap<>();
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
        if (!slotCtx.isEmpty() && temporal.hasMilestonedSlotTarget(t.pipeline())) {
            // milestoned SLOT-TARGET aliases filter by the hop context —
            // per each table's OWN dimension (cross-dimension takes
            // nothing; audit 13's own-dimension rule, now structural)
            return new NavMat(
                    temporal.filterMilestonedJoinTargets(matM.pipeline(), slotCtx),
                    matM.slotPrefixes(), matM.stripped(), subTree);
        }
        return new NavMat(matM.pipeline(), matM.slotPrefixes(),
                matM.stripped(), subTree);
    }

    /** Re-root a child's SUB-navigation tree onto the parent row: every
     * prefix (relative to the child's row) gains the child's own join
     * prefix, recursively — composition is mechanical (b_ + c_ + pk). */
    private static Map<String, Substitution.SubNav> composeSubNavPrefixes(
            String p, Map<String, Substitution.SubNav> kids) {
        if (kids.isEmpty()) {
            return kids;
        }
        Map<String, Substitution.SubNav> out = new java.util.LinkedHashMap<>();
        for (var e : kids.entrySet()) {
            Substitution.SubNav k = e.getValue();
            out.put(e.getKey(), new Substitution.SubNav(p + k.prefix(),
                    k.rowVar(), k.bindings(),
                    composeSubNavPrefixes(p, k.children())));
        }
        return out;
    }
}
