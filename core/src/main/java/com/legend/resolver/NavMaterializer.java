// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedLambda;
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
    private final CorrelatedSubselects corrSubs;

    NavMaterializer(ClassSources sources, AssociationJoins assocs,
            SyntheticHeads synthetics, CorrelatedSubselects corrSubs) {
        this.sources = sources;
        this.assocs = assocs;
        this.synthetics = synthetics;
        this.corrSubs = corrSubs;
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

    NavMat navTargetMaterialized(TemporalFrame temporal, ClassSource target, String mappingFqn,
            String targetClassFqn, @com.legend.base.Nullable String scope,
            List<List<String>> tails) {
        return navTargetMaterialized(temporal, target, mappingFqn, targetClassFqn, scope, tails,
                null, TemporalContext.NONE);
    }

    /** {@code chainPrefix}: the dotted path of the HEAD this target hangs
     * off; {@code inheritedDates}: the PARENT hop's effective context —
     * propagation flows hop-to-hop through temporal classes (engine
     * getMilestoningContextForQualifiedProperty), not only from the root. */
    NavMat navTargetMaterialized(TemporalFrame temporal, ClassSource target, String mappingFqn,
            String targetClassFqn, @com.legend.base.Nullable String scope,
            List<List<String>> tails,
            @com.legend.base.Nullable String chainPrefix, TemporalContext inherited) {
        return navTargetMaterialized(temporal, target, mappingFqn, targetClassFqn, scope,
                tails, chainPrefix, inherited, List.of());
    }

    /** {@code parkedPreds}: filter-lifted preds that will apply to THIS
     * target — their DIRECT slot-alias reads (β-inlined qualifier bodies)
     * join the demand; property-path reads ride {@code tails}. */
    NavMat navTargetMaterialized(TemporalFrame temporal, ClassSource target, String mappingFqn,
            String targetClassFqn, @com.legend.base.Nullable String scope,
            List<List<String>> tails,
            @com.legend.base.Nullable String chainPrefix, TemporalContext inherited,
            List<TypedLambda> parkedPreds) {
        return navTargetMaterialized(temporal, target, mappingFqn, targetClassFqn, scope,
                tails, chainPrefix, inherited, parkedPreds, Set.of());
    }

    /** {@code splitChains}: dotted {@code head.mid} chains read in BOTH
     * filter and projection position — a SNAPSHOT SUB-UNION step on such a
     * chain joins ONCE PER OCCURRENCE CLASS (engine per-call join
     * identity: the projection read rides its own copy and OR-fans the
     * member arms — ROW semantics, unionalias_3 vs unionalias_2). */
    /** {@code scope}: the SOURCE's scope (ClassSource.scope) — the target
     * resolves under it. */
    /** {@code target}: the step's target source, resolved by the caller
     * through THE one lookup ({@code ClassSources.navTarget}: the step's
     * routed union when it carries routes, else the class through the
     * set-id dispatch). This method materializes; it never resolves. */
    NavMat navTargetMaterialized(TemporalFrame temporal, ClassSource target, String mappingFqn,
            String targetClassFqn, @com.legend.base.Nullable String scope,
            List<List<String>> tails,
            @com.legend.base.Nullable String chainPrefix, TemporalContext inherited,
            List<TypedLambda> parkedPreds, Set<String> splitChains) {
        // H5 SET-ID DISPATCH: a route naming a specific set of a
        // (possibly rootless) multi-set target resolves through the
        // set-discriminated binding (ClassSources.getForNav).
        String prefix = java.util.Objects.requireNonNull(chainPrefix,
                "nav materialization without a set-id dispatch prefix");
        // the HEAD this target hangs off (its identity suffix included) —
        // the one extraction; set-id dispatch and the element-scope check
        // (batch 106) both read it
        String headId = prefix.substring(prefix.lastIndexOf('.') + 1);
        ClassSource t = target;
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
        // A target ~filter rides INSIDE the joined pipeline (filtered
        // subselect): the engine's own MEASURED emission — golden
        // testFilterAfterFilter: left join firmTable on (ID = FIRMID
        // and LEGALNAME = 'Firm X') (filter-in-ON) — and it MATCHES
        // isolation semantics for isEmpty (filtered-out target reads
        // as empty). NOTE (P-batches): WHERE-vs-ON equivalence PROSE
        // is banned — the placements are row-equal ONLY for
        // null-rejecting comparisons (placement addendum §8); this arm
        // stands on its golden receipt, not an argument. The toOne()-
        // pierced strict read (engine hoists the filter to the outer
        // WHERE, dropping filter-failing rows) keeps its loud wall at
        // the substitution site (task #72).
        Set<String> tSlots = Pipelines.slotAliases(t.pipeline());
        var tNavSteps = Pipelines.navSteps(t.pipeline());
        Set<String> tDemand = new LinkedHashSet<>();
        Set<String> tNavs = new LinkedHashSet<>();
        Map<String, List<List<String>>> subTails =
                new LinkedHashMap<>();
        Map<String, Set<String>> assocSubLeaves = new LinkedHashMap<>();
        // the DEEPER tail past an assoc-sub end ($x.book.desk.businessUnit
        // .legalEntity.jurisdiction: [businessUnit, legalEntity, jurisdiction]
        // past 'desk') — rides the association join as its nav tails
        Map<String, Set<List<String>>> assocSubTails = new LinkedHashMap<>();
        Set<String> memberKeyDemand = new LinkedHashSet<>();
        // ISOLATION (batch 106): tails carrying an element-scoped pred
        // leave the slot spine — head → its sub-tails (the root's rule)
        Map<String, List<List<String>>> elementReroutes = new LinkedHashMap<>();
        Set<List<String>> diverted = elementDivertedTails(t, tails, headId,
                tNavSteps, elementReroutes);
        Map<String, List<String>> embPathByAlias = new LinkedHashMap<>();
        for (List<String> tail : tails) {
            if (tail.isEmpty() || diverted.contains(tail)) {
                continue;
            }
            TypedSpec b = t.bindings().get(
                    SyntheticHeads.realHead(tail.get(0)));
            if (b == null) {
                demandUnboundTail(temporal, t, tail, mappingFqn, targetClassFqn,
                        chainPrefix, hopCtx, tDemand, memberKeyDemand, assocSubLeaves,
                        assocSubTails);
                continue;
            }
            // EMBEDDED ctor on the way to a navigate slot (batch 109): the
            // slot is demanded under the ctor's own expression; the tree
            // gains an embedded node (embPathByAlias) for the walk
            EmbeddedDrill ed = drillEmbedded(t, tail);
            List<String> slotTail = ed != null ? ed.tail() : tail;
            if (ed != null) {
                b = ed.binding();
                String edAlias = InnerDemand.navSlotAlias(b, t.rowVar(), tNavSteps.keySet());
                if (edAlias != null) {
                    embPathByAlias.putIfAbsent(edAlias, ed.embPath());
                }
            }
            CorrelatedSubselects.collectAliasReads(b, t.rowVar(), tSlots, tDemand);
            demandSlotSubTail(temporal, t, slotTail, b, tSlots,
                    tNavSteps, tDemand, tNavs, subTails, chainPrefix, hopCtx);
        }
        for (TypedLambda sp : parkedPreds) {
            for (TypedSpec sb : sp.body()) {
                CorrelatedSubselects.collectAliasReads(sb,
                        sp.parameters().get(0), tSlots, tDemand);
            }
        }
        // NOTE (#70): a demanded nav step's JOIN PREDICATE reading other
        // joinslot sub-rows (the tree optimization-table pattern) is NOT
        // demanded here on purpose — the optimization chains declare
        // (INNER) hops (orgs: @a > (INNER) @b) that our slot emission
        // does not thread yet; demanding them emits LEFT where the
        // mapping says INNER (row-count wrong: JoinIsolationDeeper
        // expected 4, got 11). The stripped-slot backstop keeps these
        // LOUD until the JoinType threading rung lands.
        tDemand = Pipelines.closeOverConditions(t.pipeline(), tDemand);
        final TemporalContext slotCtx = hopCtx;
        final Map<String, String> midByAlias = new LinkedHashMap<>();
        // SECOND head identities on one physical sub-slot — extracted at
        // the numbered seam (CodeShapeGuardrailTest)
        Map<String, String> extraSubHeads = new LinkedHashMap<>();
        Map<String, List<List<String>>> extraSubTails = new LinkedHashMap<>();
        collectExtraSubIdentities(t, tails, diverted, tNavSteps, midByAlias,
                extraSubHeads, extraSubTails);
        final Map<String, NavMat> subMats = new LinkedHashMap<>();
        final Map<String, String> subClsByAlias = new LinkedHashMap<>();
        // #70 PROJECTION-position composite (the JoinIsolationDeeper
        // family): a demanded sub-nav step whose PREDICATE reads a
        // sibling joinslot builds its COMPOSITE eagerly (target ⋈
        // slotTable ON the step condition) and the step's predicate is
        // rewritten to hop-1's oriented condition — the sibling slot
        // never joins at parent level (1:N explosion, probed).
        java.util.Map<String, TypedSpec> compositeByAlias = new java.util.LinkedHashMap<>();
        // a union target's member threads carry the demanded association
        // keys through the union projection (engine partial-union goldens)
        TypedSpec pipelineForMat = t.pipeline();
        if (Pipelines.containsConcatenate(pipelineForMat)) {
            Set<String> unionDemand = new LinkedHashSet<>(tDemand);
            unionDemand.addAll(memberKeyDemand);
            pipelineForMat = StackBuilder.demandForKeys(pipelineForMat, unionDemand);
        }
        for (String na : new java.util.ArrayList<>(tNavs)) {
            var st = tNavSteps.get(na);
            if (st == null || st.predicate().parameters().size() != 2) {
                continue;
            }
            boolean readsSibling = false;
            for (TypedSpec b3 : st.predicate().body()) {
                for (String sl : tSlots) {
                    if (Pipelines.referencesAliasOn(b3,
                            st.predicate().parameters().get(0),
                            java.util.Set.of(sl))) {
                        readsSibling = true;
                    }
                }
            }
            if (!readsSibling
                    || !(st.target() instanceof TypedGetAll ng)) {
                continue;
            }
            TypedSpec sub0 = subPipeFor(temporal, t, na, ng.classFqn(),
                    mappingFqn, subTails, midByAlias, subMats,
                    subClsByAlias, chainPrefix, hopCtx);
            CorrelatedSubselects.CompositeChain cc =
                    corrSubs.compositeChainTarget(t, st.predicate(), java.util.Objects.requireNonNull(sub0));
            if (cc == null) {
                // composite not applicable: the flat sibling-reading
                // predicate stands. NOT walled (audit 23 B6 probe): the
                // chained-union family (testUnionWithChainedJoinsAcross*
                // V2) passes row-correct through the flat form — the
                // 1:N explosion risk is multiplicity-dependent, and a
                // blanket wall over-fired on those passing shapes.
                continue;
            }
            compositeByAlias.put(na, cc.pipeline());
            TypedSpec rewrittenPfm = rewriteNavPredicate(pipelineForMat, na,
                    cc.orientedCond());
            if (rewrittenPfm == pipelineForMat) {
                // audit 23 B6: the oriented condition MUST install — a
                // step off the Navigate/JoinSlot/Filter spine would join
                // on the un-rewritten sibling-reading condition
                throw new IllegalStateException("resolver bug: navigate"
                        + " step '" + na + "' is not on the materialization"
                        + " spine — the composite's oriented condition was"
                        + " not installed");
            }
            pipelineForMat = rewrittenPfm;
        }
        final TypedSpec pfm = pipelineForMat;
        Pipelines.Materialized matM = Pipelines.materialize(
                pfm, tDemand, tNavs,
                targetClassFqn, subHopResolver(temporal, t, mappingFqn, subTails,
                        midByAlias, subMats, subClsByAlias, chainPrefix, hopCtx,
                        compositeByAlias));
        Map<String, Substitution.SubNav> subTree = new LinkedHashMap<>();
        for (var sm : subMats.entrySet()) {
            String prop = midByAlias.get(sm.getKey());
            String p = matM.slotPrefixes().get(sm.getKey());
            if (prop == null || p == null) {
                continue;
            }
            ClassSource subCs = sources.get(mappingFqn,
                    java.util.Objects.requireNonNull(
                            subClsByAlias.get(sm.getKey())), t.scope());
            Substitution.SubNav sn = new Substitution.SubNav(p, subCs.rowVar(),
                    subCs.bindings(),
                    composeSubNavPrefixes(p, sm.getValue().subNavs()));
            List<String> embPath = embPathByAlias.get(sm.getKey());
            if (embPath == null) {
                subTree.put(prop, sn);
            } else {
                putUnderEmbedded(subTree, t.bindings(), t.rowVar(), embPath, prop, sn);
            }
        }
        TypedSpec pipe = stampSlotTargets(temporal, t, matM, slotCtx,
                chainPrefix);
        pipe = foldAssocSubs(temporal, t, pipe, subTree, assocSubLeaves,
                assocSubTails, chainPrefix);
        pipe = foldExtraSubIdentities(temporal, mappingFqn, t, pipe, subTree,
                extraSubHeads, extraSubTails, tNavSteps, chainPrefix, hopCtx);
        pipe = foldProjectionCopies(temporal, mappingFqn, t, pipe, subTree,
                subMats, midByAlias, matM, subClsByAlias, subTails,
                tNavSteps, chainPrefix, hopCtx, splitChains);
        pipe = foldElementReroutes(temporal, mappingFqn, t, pipe, subTree,
                elementReroutes, tNavSteps, prefix, hopCtx);
        return new NavMat(pipe, matM.slotPrefixes(), matM.stripped(), subTree);
    }

    /** SECOND head identities on one physical sub-slot (the 2a-x rule at
     * sub depth): the slot materializes once for the FIRST identity; every
     * other identity emits its OWN prefixed join from the same nav step,
     * with its own parked pred. Fills {@code midByAlias} (alias → first
     * identity), {@code extraSubHeads} and {@code extraSubTails}. */
    private void collectExtraSubIdentities(ClassSource t, List<List<String>> tails,
            Set<List<String>> diverted,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> tNavSteps,
            Map<String, String> midByAlias, Map<String, String> extraSubHeads,
            Map<String, List<List<String>>> extraSubTails) {
        for (List<String> tail0 : tails) {
            if (diverted.contains(tail0)) {
                continue;
            }
            EmbeddedDrill ed2 = drillEmbedded(t, tail0);
            List<String> tail = ed2 != null ? ed2.tail() : tail0;
            if (tail.size() >= 2
                    || (!tail.isEmpty()
                            && assocs.toOneClassProp(t.classFqn(), tail.get(0)))) {
                TypedSpec b2 = ed2 != null ? ed2.binding() : t.bindings().get(
                        SyntheticHeads.realHead(tail.get(0)));
                String a2 = b2 == null ? null
                        : InnerDemand.navSlotAlias(b2, t.rowVar(), tNavSteps.keySet());
                if (a2 != null) {
                    midByAlias.putIfAbsent(a2, tail.get(0));
                    if (!midByAlias.get(a2).equals(tail.get(0))
                            && synthetics.correlatedPred(tail.get(0)) != null) {
                        // audit 23 B6: a CORRELATED second identity has no
                        // extra-sub emission — dropping it silently reads
                        // the FIRST identity's rows under the wrong pred
                        throw new com.legend.error.NotImplementedException(
                                "correlated filtered navigation '"
                                + tail.get(0) + "' as a SECOND identity on"
                                + " slot '" + a2 + "' is not supported yet");
                    }
                    if (!midByAlias.get(a2).equals(tail.get(0))) {
                        extraSubHeads.putIfAbsent(tail.get(0), a2);
                        List<List<String>> xt = extraSubTails.computeIfAbsent(
                                tail.get(0), k -> new ArrayList<>());
                        xt.add(tail.subList(1, tail.size()));
                        for (TypedLambda sp : synthetics.allPreds(tail.get(0))) {
                            Set<List<String>> spp = new LinkedHashSet<>();
                            for (TypedSpec sb : sp.body()) {
                                FlattenOps.consumedPaths(sb,
                                        sp.parameters().get(0), spp);
                            }
                            xt.addAll(spp);
                        }
                    }
                }
            }
        }
    }

    /**
     * ISOLATION (engine forced self-join — isolationTest, batch 106): a
     * tail whose head or FIRST sub-hop carries a correlated predicate
     * RE-BASED onto THIS target's element (SyntheticHeads.ElementScope:
     * the predicate read the outer row only through the head this target
     * hangs off — {@code $x.employees.group.children->filter(c | … ==
     * $x.employees.product.name)}) leaves the slot spine: it joins as the
     * exploding parent-copy subselect with THIS target as the parent (the
     * root's #69 shape one level down — the engine copies the element's
     * table keyed by its PK: {@code persontable_2.ID = persontable_0.ID}).
     * Collected here (head → its sub-tails, the root's navTails rule);
     * folded by {@link #foldElementReroutes}. A deeper element-scoped
     * predicate, or one on a non-navigate head, keeps a loud wall.
     */
    /** The tails the element reroute takes off the slot spine: every
     * rerouted tail plus its PREFIX tails (the demand scan records each
     * prefix of a read; a prefix that serves no other read would demand
     * the plain slot beside the reroute for nothing). */
    private Set<List<String>> elementDivertedTails(ClassSource t,
            List<List<String>> tails, String headId,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> tNavSteps,
            Map<String, List<List<String>>> out) {
        Set<List<String>> diverted = new LinkedHashSet<>();
        for (List<String> tail : tails) {
            if (!tail.isEmpty()
                    && elementRerouteTail(t, tail, headId, tNavSteps, out)) {
                diverted.add(tail);
            }
        }
        if (diverted.isEmpty()) {
            return diverted;
        }
        for (List<String> tail : tails) {
            if (tail.isEmpty() || diverted.contains(tail)) {
                continue;
            }
            boolean prefixOfDiverted = diverted.stream().anyMatch(d ->
                    d.size() > tail.size() && d.subList(0, tail.size()).equals(tail));
            boolean prefixOfKept = tails.stream().anyMatch(o ->
                    !diverted.contains(o) && o.size() > tail.size()
                    && o.subList(0, tail.size()).equals(tail));
            if (prefixOfDiverted && !prefixOfKept) {
                diverted.add(tail);
            }
        }
        return diverted;
    }

    private boolean elementRerouteTail(ClassSource t, List<String> tail,
            String headId,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> tNavSteps,
            Map<String, List<List<String>>> out) {
        if (tail.size() < 2) {
            return false;
        }
        int at = -1;
        for (int i = 0; i + 1 < tail.size(); i++) {
            SyntheticHeads.ElementScope sc = synthetics.elementScope(tail.get(i));
            if (sc != null && sc.classFqn().equals(t.classFqn())
                    && sc.head().equals(SyntheticHeads.realHead(headId))) {
                at = i;
                break;
            }
        }
        if (at < 0) {
            return false;
        }
        String shown = String.join(".", tail.stream()
                .map(SyntheticHeads::realHead).toList());
        if (at > 1) {
            throw new com.legend.error.NotImplementedException(
                    "element-scoped correlated filter predicate on hop '"
                    + SyntheticHeads.realHead(tail.get(at)) + "' at depth "
                    + (at + 1) + " of the navigation " + shown + " under '"
                    + SyntheticHeads.realHead(headId) + "' has no application"
                    + " site yet (the nested parent-copy reroute applies head"
                    + " and first-tail-hop predicates only)");
        }
        TypedSpec b = t.bindings().get(SyntheticHeads.realHead(tail.get(0)));
        if (InnerDemand.navSlotAlias(b, t.rowVar(), tNavSteps.keySet()) == null) {
            throw new com.legend.error.NotImplementedException(
                    "element-scoped correlated filter predicate on the"
                    + " navigation " + shown + " under '"
                    + SyntheticHeads.realHead(headId) + "' whose head is not a"
                    + " navigate slot of " + t.classFqn()
                    + " is not supported yet");
        }
        out.computeIfAbsent(tail.get(0), k -> new ArrayList<>())
                .add(tail.subList(1, tail.size()));
        return true;
    }

    /** The element reroutes' emission: per head, the target materialized
     * over the rerouted sub-tails (its own parked-pred reads are tails
     * too — the root's predTailsFor rule), the exploding parent-copy
     * subselect with THIS target as the parent (CorrelatedSubselects
     * .explodingSubselect: parent copy ⋈ target, WHERE the head's and the
     * first sub-hop's re-based predicates, keyed by the parent's PK),
     * LEFT-joined onto the pipeline under the head's prefix. The SubNav
     * registers under the head: a PLAIN slot demand of the same head
     * keeps its own SubNav (a different join) and gains the rerouted
     * children — the plain read and the filtered chain never share a
     * join copy. */
    private TypedSpec foldElementReroutes(TemporalFrame temporal,
            String mappingFqn, ClassSource t, TypedSpec pipe,
            Map<String, Substitution.SubNav> subTree,
            Map<String, List<List<String>>> reroutes,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> tNavSteps,
            String chainPrefix, TemporalContext hopCtx) {
        for (var e : reroutes.entrySet()) {
            String head = e.getKey();
            String alias = java.util.Objects.requireNonNull(InnerDemand.navSlotAlias(
                    t.bindings().get(SyntheticHeads.realHead(head)), t.rowVar(),
                    tNavSteps.keySet()));
            var nav = java.util.Objects.requireNonNull(tNavSteps.get(alias));
            String targetCls = ((TypedGetAll) nav.target()).classFqn();
            String subChain = chainPrefix + "." + head;
            List<List<String>> tails = new ArrayList<>(e.getValue());
            for (TypedLambda sp : synthetics.allPreds(head)) {
                Set<List<String>> spp = new LinkedHashSet<>();
                for (TypedSpec sb : sp.body()) {
                    FlattenOps.consumedPaths(sb, sp.parameters().get(0), spp);
                }
                tails.addAll(spp);
            }
            ClassSource target = sources.navTarget(t, targetCls, nav, head);
            NavMat mat = navTargetMaterialized(temporal, target, mappingFqn, targetCls,
                    t.scope(), tails, subChain, hopCtx);
            TypedSpec tPipe = temporal.temporalTargetPipe(t, target, subChain,
                    temporal.applyJoinTemporalFilters(mat.pipeline(), target,
                            Map.of()));
            tPipe = synthetics.applyToPipe(head, tPipe, (p, pred) ->
                    CorrelatedSubselects.predFilteredPipe(p, target,
                            mat.slotPrefixes(), mat.subNavs(), pred, mappingFqn));
            var leftRow = com.legend.compiler.element.type.Type
                    .requireRelationSchema(pipe.info().type());
            // the prefix bumps against the MATERIALIZED row: a plain slot
            // demand of the same head already rides it under head_
            AssociationJoins.AssocJoin aj = new AssociationJoins.AssocJoin(
                    AssociationJoins.prefixFor(head, leftRow), target, tPipe,
                    com.legend.compiler.element.type.Type.requireRelationSchema(
                            tPipe.info().type()),
                    AssociationJoins.withOuterDatedWindow(temporal, t, target,
                            subChain, nav.predicate(), tPipe),
                    mat.slotPrefixes(), mat.subNavs(),
                    synthetics.correlatedPred(head), null,
                    synthetics.isInnerValueHead(head));
            CorrelatedSubselects.ExplodingSub ex =
                    corrSubs.explodingSubselect(t, aj, leftRow);
            List<com.legend.compiler.element.type.Type.Column> cols =
                    new ArrayList<>(leftRow.columns());
            for (var c : ex.row().columns()) {
                cols.add(new com.legend.compiler.element.type.Type.Column(
                        aj.prefix() + c.name(), c.type(), c.multiplicity()));
            }
            pipe = new com.legend.compiler.spec.typed.TypedJoin(pipe, ex.target(),
                    aj.rowDropping() ? AssociationJoins.innerKind()
                            : AssociationJoins.leftKind(),
                    ex.cond(), java.util.Optional.of(aj.prefix()), null,
                    new com.legend.compiler.element.type.ExprType(
                            com.legend.compiler.element.type.Type.relation(
                                    new com.legend.compiler.element.type.Type
                                            .RelationType(cols)),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE),
                false /* resolver-synth */);
            Substitution.SubNav rerouted = new Substitution.SubNav(aj.prefix(),
                    target.rowVar(), target.bindings(),
                    composeSubNavPrefixes(aj.prefix(), mat.subNavs()));
            Substitution.SubNav plain = subTree.get(head);
            if (plain == null) {
                subTree.put(head, rerouted);
            } else {
                Map<String, Substitution.SubNav> kids =
                        new LinkedHashMap<>(plain.children());
                for (var k : rerouted.children().entrySet()) {
                    kids.putIfAbsent(k.getKey(), k.getValue());
                }
                subTree.put(head, new Substitution.SubNav(plain.prefix(),
                        plain.rowVar(), plain.bindings(), kids));
            }
        }
        return pipe;
    }



    /** Milestoned SLOT-TARGET aliases filter by the hop context — per
     * each table's OWN dimension (cross-dimension takes nothing; audit
     * 13's own-dimension rule, now structural). The chain prefix lets an
     * OUTER-READ hop date register DEFERRED windows for slots it cannot
     * stamp in-pipe (W40). */
    private static TypedSpec stampSlotTargets(TemporalFrame temporal,
            ClassSource t, Pipelines.Materialized matM,
            TemporalContext slotCtx, @com.legend.base.Nullable String chainPrefix) {
        return !slotCtx.isEmpty()
                && temporal.hasMilestonedSlotTarget(t.pipeline())
                ? temporal.filterMilestonedJoinTargets(matM.pipeline(),
                        slotCtx, chainPrefix)
                : matM.pipeline();
    }

    /** A tail whose head has NO binding on the target: an association end
     * mapped on the member sets (key demand through the union projection)
     * or the one-hop assoc-sub route (extracted seam of
     * navTargetMaterialized). */
    private void demandUnboundTail(TemporalFrame temporal, ClassSource t,
            List<String> tail, String mappingFqn, String targetClassFqn,
            @com.legend.base.Nullable String chainPrefix, TemporalContext hopCtx,
            Set<String> tDemand, Set<String> memberKeyDemand,
            Map<String, Set<String>> assocSubLeaves,
            Map<String, Set<List<String>>> assocSubTails) {
            // an ASSOCIATION end mapped on the MEMBER sets of this
            // union / inheritance target (no hoisted binding): the
            // outer association join reads the members' key columns
            // off this row — demand them through the union projection
            // (group F burn 2026-09-02)
            if (tail.size() == 1 && assocs.assocTargetClassOf(targetClassFqn,
                    SyntheticHeads.realHead(tail.get(0))).isPresent()) {
                Set<String> keyReads = assocs.memberAssocKeyReads(
                        mappingFqn, t, tail.get(0));
                tDemand.addAll(keyReads);
                memberKeyDemand.addAll(keyReads);
            }
            // ASSOC-SUB (union V3): the tail continues through an
            // ASSOCIATION end on this target (head y is a nav slot, z
            // on plain Y realizes via the association route). A DEEPER
            // tail (F-M, 2026-09-16: book.desk.businessUnit.legalEntity
            // .jurisdiction — 1,796 stress rows) rides the association
            // join as its nav tails: the sub-target's own slots
            // materialize recursively and the SubNav tree carries them.
            // Context-less temporal targets keep their loud wall.
            if (tail.size() >= 2) {
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
                        && !Pipelines.containsConcatenate(sources
                                .get(mappingFqn, subClsOpt.get(), t.scope())
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
                        if (tail.size() > 2) {
                            assocSubTails.computeIfAbsent(tail.get(0),
                                    k -> new LinkedHashSet<>())
                                    .add(tail.subList(1, tail.size()));
                        }
                    }
                }
            }
    }

    /** ONE demanded sub-nav target pipeline: recursive materialization,
     * lifted-pred application, per-hop temporal stamping (the materialize
     * resolver body, extracted so composites can pre-build). */

    /** The sub-hop target resolver of one materialization: sub pipes by
     * alias (composited steps first), and the parent-scoped predicate
     * composition on a sub-hop's join condition. */
    private Pipelines.TargetResolver subHopResolver(TemporalFrame temporal,
            ClassSource t, String mappingFqn,
            Map<String, List<List<String>>> subTails,
            Map<String, String> midByAlias, Map<String, NavMat> subMats,
            Map<String, String> subClsByAlias, @com.legend.base.Nullable String chainPrefix,
            TemporalContext hopCtx, Map<String, TypedSpec> compositeByAlias) {
        return new Pipelines.TargetResolver() {
            @Override
            public TypedSpec pipelineFor(String alias, String cls) {
                return compositeByAlias.containsKey(alias)
                        ? java.util.Objects.requireNonNull(
                                compositeByAlias.get(alias))
                        : java.util.Objects.requireNonNull(
                                subPipeFor(temporal, t, alias, cls,
                                        mappingFqn, subTails,
                                        midByAlias, subMats,
                                        subClsByAlias, chainPrefix,
                                        hopCtx),
                                () -> "sub-navigation '" + alias
                                        + "' has no materializable"
                                        + " pipeline");
            }

            /** A sub-hop whose lifted predicate is correlated to
             * THIS target's row (a mapper-scoped lift: `$b.trades
             * ->map(t | $t.products->filter(p | $p.date == $t.d)
             * ...)` — the pred's outer reads are the parent hop's
             * plain properties) composes into the sub-hop's ON
             * clause: the engine's nested join with the filter in
             * the join condition (injection
             * testProjectThroughAssociation). */
            @Override
            public TypedLambda conditionFor(String alias, TypedLambda cond) {
                String midProp = midByAlias.get(alias);
                if (midProp == null || !synthetics.isParentScoped(midProp)) {
                    return cond;
                }
                TypedLambda pred = synthetics.correlatedPred(midProp);
                NavMat sm = subMats.get(alias);
                String cls = subClsByAlias.get(alias);
                if (pred == null || sm == null || cls == null) {
                    return cond;
                }
                return assocs.andCorrelatedIntoCondition(cond, pred, t,
                        sources.navTarget(t, cls, ClassSources.stepOf(t, alias),
                                midProp == null ? alias : midProp),
                        sm.slotPrefixes());
            }
};
    }

    private @com.legend.base.Nullable TypedSpec subPipeFor(TemporalFrame temporal, ClassSource t,
            String alias, String cls, String mappingFqn,
            Map<String, List<List<String>>> subTails,
            Map<String, String> midByAlias, Map<String, NavMat> subMats,
            Map<String, String> subClsByAlias, @com.legend.base.Nullable String chainPrefix,
            TemporalContext hopCtx) {

            String midProp = midByAlias.get(alias);
            ClassSource subTarget = sources.navTarget(t, cls, ClassSources.stepOf(t, alias),
                    midProp == null ? alias : midProp);
            NavMat subMat = navTargetMaterialized(temporal, subTarget, mappingFqn, cls, t.scope(),
                    subTails.getOrDefault(alias, List.of()),
                    chainPrefix == null ? null
                            : chainPrefix + "." + midProp,
                    hopCtx,
                    midProp == null ? List.of()
                            : synthetics.allPreds(midProp));
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
                        (pp, pred) -> CorrelatedSubselects.predFilteredPipe(
                                pp, subTarget,
                                sm2.slotPrefixes(), sm2.subNavs(),
                                pred, mappingFqn));
            }
            // per-hop temporal filter: the sub-hop's chain-keyed
            // spec or propagated context (parent = THIS target)
            if (temporal.temporalStrategy(cls) != null && chainPrefix != null) {
                String subChain = chainPrefix + "." + midByAlias.get(alias);
                TemporalFrame.TemporalSpec subSpec = temporal.spec(subChain);
                if (subSpec != null) {
                    sub = temporal.temporalTargetPipe(t, subTarget, subChain, sub);
                } else {
                    // DIMENSION-PROJECTED inheritance through a
                    // TEMPORAL parent (contextAt clears through
                    // non-temporal hops structurally — audit 13
                    // F4/F5), stamped by the sub CLASS's own
                    // temporality (bitemp pair / point / range)
                    sub = temporal.stampForClassOrDefer(sub,
                            temporal.contextAt(subChain, cls, hopCtx), cls,
                            subChain);
                }
            }
            return sub;
    }


    /** The pipeline with one navigate step's predicate REPLACED (the
     * composite's oriented hop-1 condition) — shared by the sub-level
     * composite arm and the top-level per-occurrence bundling
     * (StoreResolver, §4AD batch 5). */
    static TypedSpec rewriteNavPredicate(TypedSpec pipe, String alias,
            com.legend.compiler.spec.typed.TypedLambda cond) {
        if (pipe instanceof com.legend.compiler.spec.typed.TypedNavigate nav
                && nav.alias().isPresent()
                && nav.alias().get().equals(alias)) {
            return new com.legend.compiler.spec.typed.TypedNavigate(
                    rewriteNavPredicate(nav.source(), alias, cond),
                    nav.alias(), nav.target(), cond, nav.form(), nav.info());
        }
        if (pipe instanceof com.legend.compiler.spec.typed.TypedNavigate nav) {
            TypedSpec src = rewriteNavPredicate(nav.source(), alias, cond);
            return src == nav.source() ? pipe
                    : new com.legend.compiler.spec.typed.TypedNavigate(src,
                            nav.alias(), nav.target(), nav.predicate(),
                            nav.form(), nav.info());
        }
        if (pipe instanceof com.legend.compiler.spec.typed.TypedJoinSlot js) {
            TypedSpec src = rewriteNavPredicate(js.source(), alias, cond);
            return src == js.source() ? pipe
                    : new com.legend.compiler.spec.typed.TypedJoinSlot(src,
                            js.alias(), js.target(), js.condition(), js.frameName(), js.info());
        }
        if (pipe instanceof com.legend.compiler.spec.typed.TypedFilter f) {
            TypedSpec src = rewriteNavPredicate(f.source(), alias, cond);
            return src == f.source() ? pipe
                    : new com.legend.compiler.spec.typed.TypedFilter(src,
                            f.predicate(), f.info());
        }
        return pipe;
    }

    /** The SLOT sub-route demand for one 2+-hop tail: gates (temporal /
     * filtered / correlated), sub-alias demand + sub-tails, and the
     * lifted-pred tails (extracted seam of navTargetMaterialized). */
    private void demandSlotSubTail(TemporalFrame temporal, ClassSource t,
            List<String> tail, TypedSpec b, Set<String> tSlots,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> tNavSteps,
            Set<String> tDemand, Set<String> tNavs,
            Map<String, List<List<String>>> subTails,
            @com.legend.base.Nullable String chainPrefix, TemporalContext hopCtx) {
        String mappingFqn = t.mappingFqn();
        // a BARE class-typed TO-ONE tail joins too (qualifier-truncated
        // demand — the qualifier body's leaves never reach the scan, but
        // the SubNav's full binding table resolves them at substitution)
        if (tail.size() >= 2
                || assocs.toOneClassProp(t.classFqn(), tail.get(0))) {
            // a CORRELATED sub-hop pred does NOT park in-target
            // (applyToPipe reads closed preds only — the sub pipe stays
            // unfiltered HERE); it applies in the exploding parent-copy
            // sub's WHERE (the tail-pred loop) — the reroute trigger
            // diverts every such chain there, so no route consumes the
            // unfiltered join.
            String subAlias = InnerDemand.navSlotAlias(b, t.rowVar(), tNavSteps.keySet());
            if (subAlias != null) {
                // audit 12 F2: a TEMPORAL (or gated) sub-target must NOT
                // materialize unfiltered under a non-temporal parent —
                // the recursion's own gate returns a raw pipeline but
                // cannot stop THIS level's join. Leave the sub-step
                // undemanded: the leaf read stays LOUD downstream.
                var subStep = java.util.Objects.requireNonNull(tNavSteps.get(subAlias));
                String subCls = ((TypedGetAll) subStep.target()).classFqn();
                ClassSource subT = sources.navTarget(t, subCls, subStep, subAlias);
                // TEMPORAL sub-target: liftable when its CHAIN-KEYED
                // spec (explicit hop date) or propagated context can
                // filter it (temporalTargetPipe in the resolver lambda
                // below); no chain prefix or no context = stays loud.
                boolean temporalSub = temporal.temporalStrategy(subCls) != null;
                if (temporalSub && (chainPrefix == null
                        // a chain-keyed SPEC of any form (point, range
                        // sweep) is a usable context — temporalTargetPipe
                        // in the resolver lambda handles each; only the
                        // spec-less no-propagation case stays loud.
                        // SNAPSHOT sub-unions share the chain-keyed join
                        // too: the spec registry is keyed by THIS chain, so
                        // every read that reaches here filters by ONE date —
                        // the shared join is row-identical to the engine's
                        // per-call fan (its 16 = our 8 x 2 is join-COUNT
                        // shape, not rows; two-dates-per-head collides in
                        // the spec registry before this route and stays its
                        // own rung).
                        || (temporal.spec(
                                chainPrefix + "." + tail.get(0)) == null
                            && temporal.contextAt(chainPrefix + "." + tail.get(0),
                                subCls, hopCtx).isEmpty()))) {
                    return;
                }
                // milestoned SLOT TARGETS inside the sub's own pipeline
                // are filterable when the SUB hop has a date context —
                // the recursion's own slotDates/filterMilestonedJoin-
                // Targets pass stamps them (audit 14 ungate: the
                // blanket gate predated per-hop context threading);
                // context-less they'd fan versions out — stays loud
                // for reads that actually CROSS one (demand-aware: a
                // scalar tail beside an un-demanded milestoned slot is
                // safe — materialization never touches the slot)
                boolean subHasContext = chainPrefix != null
                        && (temporal.spec(
                                chainPrefix + "." + tail.get(0)) != null
                            || !temporal.contextAt(
                                chainPrefix + "." + tail.get(0),
                                subCls, hopCtx).isEmpty());
                Set<String> subMilestoned =
                        temporal.milestonedSlotAliases(subT.pipeline());
                if (!subMilestoned.isEmpty() && !subHasContext) {
                    Set<String> subDemand = new LinkedHashSet<>();
                    Set<String> subSlots =
                            Pipelines.slotAliases(subT.pipeline());
                    for (String leaf : tail.subList(1, tail.size())) {
                        TypedSpec lb = subT.bindings().get(
                                SyntheticHeads.realHead(leaf));
                        if (lb != null) {
                            CorrelatedSubselects.collectAliasReads(lb,
                                    subT.rowVar(), subSlots, subDemand);
                        }
                    }
                    subDemand = Pipelines.closeOverConditions(
                            subT.pipeline(), subDemand);
                    if (!java.util.Collections.disjoint(subDemand,
                            subMilestoned)) {
                        return;
                    }
                }
                tNavs.add(subAlias);
                subTails.computeIfAbsent(subAlias, k -> new ArrayList<>())
                        .add(tail.subList(1, tail.size()));
                // a filter-LIFTED sub-hop's parked pred reads are
                // TAILS too: they pull the sub-target's own slots
                // exactly like demanded leaves (the top-level route's
                // predTails rule, mirrored — an undemanded pred slot
                // read trips the stripped-slot backstop)
                for (TypedLambda sp : synthetics.allPreds(tail.get(0))) {
                    Set<List<String>> spp = new LinkedHashSet<>();
                    for (TypedSpec sb : sp.body()) {
                        FlattenOps.consumedPaths(sb,
                                sp.parameters().get(0), spp);
                    }
                    subTails.get(subAlias).addAll(spp);
                }
            }
        }
    }

    /** ASSOC-SUB folds (union V3): each collected end joins its target
     * INSIDE the materialized pipeline (the same descriptor->emission the
     * root uses) and rides the SubNav tree — the composed prefix (y_ + z_)
     * resolves the leaf on the joined row. */
    private TypedSpec foldAssocSubs(TemporalFrame temporal, ClassSource t,
            TypedSpec pipe, Map<String, Substitution.SubNav> subTree,
            Map<String, Set<String>> assocSubLeaves,
            Map<String, Set<List<String>>> assocSubTails,
            @com.legend.base.Nullable String chainPrefix) {
        for (var e : assocSubLeaves.entrySet()) {
            String prop = e.getKey();
            String subChain = chainPrefix == null ? prop
                    : chainPrefix + "." + prop;
            AssociationJoins.AssocJoin aj = assocs.associationJoin(temporal,
                    t, prop, StoreResolver.Context.NONE, false,
                    e.getValue(), subChain,
                    assocSubTails.getOrDefault(prop, Set.of()));
            // the sub-join's condition reads this target's KEY off the
            // left row — a ROUTED target projected it under the route
            // slot and dropped the column (D_PaymentDense: OTC_ID);
            // demand it back before the join binds (the F-O seam,
            // ledger F-AD)
            pipe = StackBuilder.demandForCondition(pipe,
                    aj.onForm() != null ? aj.onForm().condition() : aj.condition(), 0);
            var leftRow = com.legend.compiler.element.type.Type
                    .requireRelationSchema(pipe.info().type());
            List<com.legend.compiler.element.type.Type.Column> cols =
                    new ArrayList<>(leftRow.columns());
            for (var c : aj.targetRow().columns()) {
                cols.add(new com.legend.compiler.element.type.Type.Column(
                        aj.prefix() + c.name(), c.type(), c.multiplicity()));
            }
            // ENGINE ON-FORM opt-in (plain LEFT-join emitter): the
            // temporal window spells in the join condition, pipe raw
            TypedSpec ajPipe = aj.onForm() != null
                    ? aj.onForm().pipeline() : aj.targetPipeline();
            com.legend.compiler.spec.typed.TypedLambda ajCond =
                    aj.onForm() != null
                    ? aj.onForm().condition()
                    : java.util.Objects.requireNonNull(aj.condition(), "aj.condition()");
            pipe = new com.legend.compiler.spec.typed.TypedJoin(pipe,
                    ajPipe, AssociationJoins.leftKind(),
                    ajCond, java.util.Optional.of(aj.prefix()), null,
                    new com.legend.compiler.element.type.ExprType(
                            com.legend.compiler.element.type.Type.relation(
                                    new com.legend.compiler.element.type.Type
                                            .RelationType(cols)),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE),
                false /* resolver-synth */);
            // the join's own sub-tree (the target's slots the deeper tail
            // demanded), composed under this end's prefix — relative to
            // THIS target's row like every SubNav at this level
            subTree.put(prop, new Substitution.SubNav(aj.prefix(),
                    aj.target().rowVar(), aj.target().bindings(),
                    composeSubNavPrefixes(aj.prefix(), aj.targetSubNavs())));
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
            @com.legend.base.Nullable String chainPrefix, TemporalContext hopCtx) {
        for (var e : extraSubHeads.entrySet()) {
            String prop = e.getKey();
            String alias = e.getValue();
            var step = java.util.Objects.requireNonNull(
                    tNavSteps.get(alias));
            if (!(step.target() instanceof TypedGetAll xg)) {
                continue;
            }
            String subChain = chainPrefix == null ? prop
                    : chainPrefix + "." + prop;
            ClassSource xCs = sources.navTarget(t, xg.classFqn(), step, prop);
            NavMat xMat = navTargetMaterialized(temporal, xCs, mappingFqn,
                    xg.classFqn(), t.scope(),
                    extraSubTails.getOrDefault(prop, List.of()),
                    subChain, hopCtx, synthetics.allPreds(prop));
            final NavMat xm2 = xMat;
            TypedSpec xPipe = synthetics.applyToPipe(prop, xMat.pipeline(),
                    (pp, pred) -> CorrelatedSubselects.predFilteredPipe(
                            pp, xCs, xm2.slotPrefixes(), xm2.subNavs(),
                            pred, mappingFqn));
            // the #70 COMPOSITE for the extra identity too (batch 79): a
            // step whose predicate reads a sibling joinslot (the tree
            // optimization-table chain `orgs: @a > (INNER) @b`) joins
            // target ⋈ slotTable on the ORIENTED condition — its own copy
            // of the whole chain, exactly as the first identity's. Joining
            // the bare filtered target on the step's sibling-reading
            // predicate read the FIRST identity's slot row instead, whose
            // tree rows are the first identity's FILTERED ancestors (TEAM)
            // — the second qualifier (BUSINESS UNIT) could never match
            // (testJoinIsolationDeeperTwoIsolations: 'OrgName2' came back
            // empty). The engine copies the chain per qualifier
            // (orgtreeoptimizationtable_0 / _2).
            com.legend.compiler.spec.typed.TypedLambda xCond = step.predicate();
            CorrelatedSubselects.CompositeChain xcc =
                    corrSubs.compositeChainTarget(t, step.predicate(), xPipe);
            if (xcc != null) {
                xPipe = xcc.pipeline();
                xCond = xcc.orientedCond();
            }
            xPipe = StackBuilder.demandForCondition(xPipe, xCond, 1);
            // the synthetic identity's own suffix keys the join prefix
            // (synonyms#f1 -> alias_f1_) — deterministic, collision-free
            // per identity by construction
            // audit 23 B6: a PLAIN second identity keys by its own
            // property name — the old literal "x" collided when two
            // plain identities shared one physical slot
            String xPrefix = alias + "_"
                    + (prop.indexOf('#') >= 0
                            ? prop.substring(prop.indexOf('#') + 1)
                            : prop) + "_";
            var xLeftRow = com.legend.compiler.element.type.Type
                    .requireRelationSchema(pipe.info().type());
            var xRow = com.legend.compiler.element.type.Type
                    .requireRelationSchema(xPipe.info().type());
            List<com.legend.compiler.element.type.Type.Column> xCols =
                    new ArrayList<>(xLeftRow.columns());
            for (var c : xRow.columns()) {
                xCols.add(new com.legend.compiler.element.type.Type.Column(
                        xPrefix + c.name(), c.type(), c.multiplicity()));
            }
            pipe = new com.legend.compiler.spec.typed.TypedJoin(pipe,
                    xPipe, AssociationJoins.leftKind(),
                    xCond, java.util.Optional.of(xPrefix), null,
                    new com.legend.compiler.element.type.ExprType(
                            com.legend.compiler.element.type.Type.relation(
                                    new com.legend.compiler.element.type.Type
                                            .RelationType(xCols)),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE),
                false /* resolver-synth */);
            subTree.put(prop, new Substitution.SubNav(xPrefix,
                    xCs.rowVar(), xCs.bindings(),
                    composeSubNavPrefixes(xPrefix, xMat.subNavs())));
        }
        return pipe;
    }


    /**
     * OCCURRENCE-SPLIT (engine per-call join identity): for each demanded
     * sub-step on a {@code splitChains} chain whose target is a SNAPSHOT
     * SUB-UNION, append a SECOND prefixed LEFT join of the same (stamped)
     * sub pipeline — the projection occurrence's own copy. Substitution
     * routes projection-position reads to the {@code <prop>#p} SubNav; the
     * copy's raw OR condition fans the member arms exactly like the
     * engine's unionalias_3 (expected 16 = filtered 8 x 2).
     */
    private TypedSpec foldProjectionCopies(TemporalFrame temporal,
            String mappingFqn, ClassSource t, TypedSpec pipe,
            Map<String, Substitution.SubNav> subTree,
            Map<String, NavMat> subMats, Map<String, String> midByAlias,
            Pipelines.Materialized matM, Map<String, String> subClsByAlias,
            Map<String, List<List<String>>> subTails,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> tNavSteps,
            @com.legend.base.Nullable String chainPrefix, TemporalContext hopCtx,
            Set<String> splitChains) {
        if (System.getenv("LEGEND_LITE_SPLIT_TRACE") != null) {
            System.err.println("[split] chainPrefix=" + chainPrefix
                    + " splitChains=" + splitChains + " subMats="
                    + subMats.keySet() + " mids=" + midByAlias
                    + " prefixes=" + matM.slotPrefixes().keySet());
        }
        if (splitChains.isEmpty() || chainPrefix == null) {
            return pipe;
        }
        for (var sm : new ArrayList<>(subMats.entrySet())) {
            String na = sm.getKey();
            String prop = midByAlias.get(na);
            String base = matM.slotPrefixes().get(na);
            if (prop == null || base == null
                    || !splitChains.contains(chainPrefix + "." + prop)) {
                continue;
            }
            var st = tNavSteps.get(na);
            if (st == null || !(st.target() instanceof TypedGetAll sg)) {
                continue;
            }
            TypedSpec sub2 = subPipeFor(temporal, t, na, sg.classFqn(),
                    mappingFqn, subTails, midByAlias, subMats,
                    subClsByAlias, chainPrefix, hopCtx);
            if (sub2 == null) {
                // PROBE semantics, unlike the :297 requireNonNull: this
                // loop scans candidate tails and a null sub-pipe means
                // "not the liftable shape", not a broken invariant
                continue;
            }
            if (System.getenv("LEGEND_LITE_SPLIT_TRACE") != null) {
                System.err.println("[split] gate na=" + na + " concat="
                        + Pipelines.containsConcatenate(sub2)
                        + " snap=" + temporal.hasSnapshotScan(sub2));
            }
            if (!(Pipelines.containsConcatenate(sub2)
                    && temporal.hasSnapshotScan(sub2))) {
                continue;
            }
            // the copy's condition is EXACTLY the first copy's (the OR over
            // the member arms, already oriented by the materialization) —
            // the raw step predicate would re-resolve against the first
            // copy's columns and arm-pair (t8.type = t11.type_1, fan x1)
            var firstJoin = joinWithPrefix(matM.pipeline(), base);
            if (firstJoin == null) {
                continue;
            }
            // the copy reads the same routed keys the first copy's
            // condition binds on: member columns the union arms project
            // only on demand (B3.1)
            sub2 = StackBuilder.demandForCondition(sub2, firstJoin.condition(), 1);
            String prefix2 = na + "_p_";
            var leftRow = com.legend.compiler.element.type.Type
                    .requireRelationSchema(pipe.info().type());
            var subRow = com.legend.compiler.element.type.Type
                    .requireRelationSchema(sub2.info().type());
            List<com.legend.compiler.element.type.Type.Column> cols =
                    new ArrayList<>(leftRow.columns());
            for (var c : subRow.columns()) {
                cols.add(new com.legend.compiler.element.type.Type.Column(
                        prefix2 + c.name(), c.type(), c.multiplicity()));
            }
            pipe = new com.legend.compiler.spec.typed.TypedJoin(pipe, sub2,
                    AssociationJoins.leftKind(), firstJoin.condition(),
                    java.util.Optional.of(prefix2), null,
                    new com.legend.compiler.element.type.ExprType(
                            com.legend.compiler.element.type.Type.relation(
                                    new com.legend.compiler.element.type.Type
                                            .RelationType(cols)),
                            com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ONE),
                false /* resolver-synth */);
            ClassSource subCs = sources.get(mappingFqn,
                    java.util.Objects.requireNonNull(subClsByAlias.get(na)), t.scope());
            subTree.put(prop + "#p", new Substitution.SubNav(prefix2,
                    subCs.rowVar(), subCs.bindings(),
                    composeSubNavPrefixes(prefix2,
                            java.util.Objects.requireNonNull(subMats.get(na)).subNavs())));
        }
        return pipe;
    }

    /** The materialized join carrying {@code prefix} (the first copy of a
     * split sub-step); null when the shape holds no such join. */
    private static com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedJoin joinWithPrefix(
            TypedSpec pipe, String prefix) {
        if (pipe instanceof com.legend.compiler.spec.typed.TypedJoin j
                && j.prefix().map(prefix::equals).orElse(false)) {
            return j;
        }
        for (TypedSpec c : pipe.children()) {
            var r = joinWithPrefix(c, prefix);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    /** A tail drilled through the EMBEDDED ctor(s) its head binds to
     * ({@code $s.incomeFunction.Classification.name} where
     * {@code incomeFunction} is {@code ^IncomeFunction(code: …,
     * Classification: $row.<slot>)}): {@code embPath} = the ctor
     * components walked, {@code tail} = the rest from the navigate-slot
     * property on, {@code binding} = that property's expression (the
     * slot read the demand machinery resolves). The same drill
     * StoreResolver.registerNavigations applies to an embedded HEAD —
     * here one level down, so the ctor's class-typed Join sub-PM
     * materializes inside the sub-target (batch 109,
     * testToManyWithQualifierWithFilterOnJoin). */
    private record EmbeddedDrill(List<String> embPath, List<String> tail,
            TypedSpec binding) {}

    private static @com.legend.base.Nullable EmbeddedDrill drillEmbedded(ClassSource t,
            List<String> tail) {
        if (tail.size() < 2) {
            return null;
        }
        TypedSpec drill = t.bindings().get(SyntheticHeads.realHead(tail.get(0)));
        if (drill == null) {
            return null;
        }
        int mid = 1;
        while (true) {
            TypedSpec inner = Pipelines.unwrapToOne(drill);
            if (!(inner instanceof com.legend.compiler.spec.typed.TypedNewInstance ni)
                    || mid + 1 >= tail.size()) {
                break;
            }
            TypedSpec next = ni.properties().get(
                    SyntheticHeads.realHead(tail.get(mid)));
            if (next == null) {
                break;
            }
            drill = next;
            mid++;
        }
        if (mid == 1) {
            return null;   // the head is not an embedded ctor on this path
        }
        return new EmbeddedDrill(tail.subList(0, mid - 1),
                tail.subList(mid - 1, tail.size()), drill);
    }

    /** Register {@code sn} under the EMBEDDED node(s) of {@code embPath}
     * in the tree: an embedded ctor shares the parent's row (prefix "",
     * the parent's row var), its bindings are the ctor's own properties,
     * its children the navigate slots reached through it — the walk
     * descends hop by hop, no dotted keys. Nodes are rebuilt (records);
     * deeper ctor nesting recurses on the ctor's own property map. */
    private static void putUnderEmbedded(Map<String, Substitution.SubNav> tree,
            Map<String, TypedSpec> bindings, String rowVar,
            List<String> embPath, String prop, Substitution.SubNav sn) {
        String key = embPath.get(0);
        Substitution.SubNav node = tree.get(key);
        Map<String, TypedSpec> props = node != null ? node.bindings()
                : ctorProps(bindings.get(SyntheticHeads.realHead(key)));
        if (node == null) {
            node = new Substitution.SubNav("", rowVar, props, Map.of());
        }
        Map<String, Substitution.SubNav> kids = new LinkedHashMap<>(node.children());
        if (embPath.size() == 1) {
            kids.put(prop, sn);
        } else {
            putUnderEmbedded(kids, props, rowVar,
                    embPath.subList(1, embPath.size()), prop, sn);
        }
        tree.put(key, new Substitution.SubNav(node.prefix(), node.rowVar(),
                node.bindings(), kids));
    }

    private static Map<String, TypedSpec> ctorProps(@com.legend.base.Nullable TypedSpec expr) {
        return expr != null && Pipelines.unwrapToOne(expr)
                instanceof com.legend.compiler.spec.typed.TypedNewInstance ni
                ? ni.properties() : Map.of();
    }

    /** Re-root a child's SUB-navigation tree onto the parent row: every
     * prefix (relative to the child's row) gains the child's own join
     * prefix, recursively — composition is mechanical (b_ + c_ + pk). */
    static Map<String, Substitution.SubNav> composeSubNavPrefixes(
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
