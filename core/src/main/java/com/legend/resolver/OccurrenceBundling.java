// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PER-OCCURRENCE MID-HOP BUNDLING (§4AD batch 5, merged batch 3): a nav
 * step whose condition reads a sibling joinslot (the MID of a chained
 * PM) normally demands that slot at PARENT level — shared by every
 * identity. When the step carries MULTIPLE filtered identities, the
 * shared mid CROSS-FANS the occurrences (testProjectMerge 3 → 10); the
 * engine bundles mid ⋈ target inside each occurrence's own subselect
 * keyed on the parent (batch-0 golden). This class owns exactly that
 * routing decision + its emission plumbing; every other shape keeps the
 * pre-flip byte-stable emission.
 */
final class OccurrenceBundling {

    private OccurrenceBundling() {
    }

    /** May {@code alias} take the per-occurrence bundling route? The
     * step must carry MULTIPLE identities, every one of them a
     * filter-lifted (closed-pred) head — mixed plain/correlated groups
     * keep today's shared-slot emission (their reads still demand the
     * mid at parent level). */
    static boolean perOccurrenceBundles(SyntheticHeads synthetics,
            String alias, Map<String, String> navHeadByAlias,
            Map<String, String> extraNavHeads) {
        String base = navHeadByAlias.getOrDefault(alias, alias);
        List<String> heads = new ArrayList<>();
        heads.add(base);
        for (var e : extraNavHeads.entrySet()) {
            if (e.getValue().equals(alias)) {
                heads.add(e.getKey());
            }
        }
        if (heads.size() < 2) {
            return false;
        }
        for (String h : heads) {
            String k = h.substring(h.lastIndexOf('.') + 1);
            if (!synthetics.hasPred(k)
                    || synthetics.correlatedPred(k) != null) {
                return false;
            }
        }
        return true;
    }

    /** Does {@code nav}'s condition read any sibling joinslot? */
    static boolean readsSiblingSlot(TypedNavigate nav,
            Set<String> slotAliases) {
        for (TypedSpec b : nav.predicate().body()) {
            for (String slot : slotAliases) {
                if (Pipelines.referencesAliasOn(b,
                        nav.predicate().parameters().get(0),
                        Set.of(slot))) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Demand the sibling slots {@code nav}'s condition reads (the
     * pre-flip parent-level route). */
    static void demandSiblingSlots(TypedNavigate nav,
            Set<String> slotAliases, Set<String> demanded) {
        for (TypedSpec b : nav.predicate().body()) {
            for (String slot : slotAliases) {
                if (Pipelines.referencesAliasOn(b,
                        nav.predicate().parameters().get(0),
                        Set.of(slot))) {
                    demanded.add(slot);
                }
            }
        }
    }

    /** Composite this identity's frame (mid ⋈ filtered-target with
     * hop-1's oriented condition — the #70 machinery at TOP level,
     * upstream-slot reads allowed for DEEP chains): the route was
     * decided ONCE by {@link #perOccurrenceBundles}; a shape the
     * composite cannot build WALLS loudly (never a fallback — user
     * tenet audit 2026-08-29). Upstream slots the ORIENTED condition
     * still reads stay demanded at parent level (the engine's own
     * shape: frames join onto the shared upstream hop). Returns the
     * re-closed demand set. */
    static Set<String> composite(ClassSource cs,
            CorrelatedSubselects corrSubs, String alias,
            Map<String, TypedNavigate> navSteps,
            Map<String, NavMaterializer.NavMat> navMats,
            Map<String, TypedLambda> compositeConds,
            Set<String> demanded, Set<String> slotAliases) {
        var nav = java.util.Objects.requireNonNull(navSteps.get(alias));
        var mat = java.util.Objects.requireNonNull(navMats.get(alias));
        CorrelatedSubselects.CompositeChain cc =
                corrSubs.compositeChainTarget(cs, nav.predicate(),
                        mat.pipeline(), true);
        if (cc == null) {
            throw new com.legend.error.NotImplementedException(
                    "per-occurrence mid-hop bundling: composite not"
                    + " applicable for navigate step '" + alias
                    + "' — the multi-identity chained shape is not"
                    + " built yet");
        }
        navMats.put(alias, new NavMaterializer.NavMat(cc.pipeline(),
                mat.slotPrefixes(), mat.stripped(), mat.subNavs()));
        compositeConds.put(alias, cc.orientedCond());
        for (TypedSpec b : cc.orientedCond().body()) {
            for (String slot : slotAliases) {
                if (Pipelines.referencesAliasOn(b,
                        cc.orientedCond().parameters().get(0),
                        Set.of(slot))) {
                    demanded.add(slot);
                }
            }
        }
        return Pipelines.closeOverConditions(cs.pipeline(), demanded);
    }

    /** The EXTRA identity's frame + join condition, composited with its
     * OWN mid copy when the step was composited (falls back to the raw
     * step predicate + given pipe when infeasible would be a resolver
     * bug here — the base identity already composited feasibly). */
    record ExtraComposite(TypedSpec pipe, TypedLambda cond) {}

    static ExtraComposite extraComposite(ClassSource cs,
            CorrelatedSubselects corrSubs, boolean composited,
            TypedNavigate nav, TypedSpec tPipe, String headKey,
            String alias) {
        if (!composited) {
            return new ExtraComposite(tPipe, nav.predicate());
        }
        CorrelatedSubselects.CompositeChain cc =
                corrSubs.compositeChainTarget(cs, nav.predicate(), tPipe,
                        true);
        if (cc == null) {
            throw new com.legend.error.NotImplementedException(
                    "per-occurrence mid-hop bundling: composite not"
                    + " applicable for extra identity '" + headKey
                    + "' on step '" + alias + "'");
        }
        return new ExtraComposite(cc.pipeline(), cc.orientedCond());
    }

    /** OTHERWISE per-leaf dispatch (V1 §D.5, hosted here for the
     * StoreResolver shape limit): an embedded-partial leaf reads the
     * PARENT row — {@code null} means no join demand (the caller skips
     * the path). KIND-aware (ledger cluster 50): membership in the
     * partial proves same-row ONLY for a genuine column read; a
     * CLASS-TYPED navigate-slot member (structural Join sub-PM) returns
     * the PARTIAL so the ctor drill descends and registers the dotted
     * AssocSub (the partial's own mapping wins over the otherwise
     * target's); any other leaf demands the FALLBACK's navigate slot. */
    static @com.legend.base.Nullable TypedSpec otherwiseNavRead(
            TypedSpec headBinding, List<String> path, ClassSource cs,
            Set<String> navStepKeys) {
        var ow = Substitution.otherwiseOf(headBinding);
        if (ow == null) {
            return headBinding;
        }
        var partial = (com.legend.compiler.spec.typed.TypedNewInstance)
                ow.args().get(0);
        TypedSpec pb = partial.properties().get(
                SyntheticHeads.realHead(path.get(1)));
        if (pb == null) {
            return ow.args().get(1);
        }
        return InnerDemand.navSlotAlias(pb, cs.rowVar(), navStepKeys) == null
                ? null : partial;
    }

    /** A FINGERPRINTED identity prefixes {@code alias_dN_}
     * (NavMaterializer xPrefix rule at root depth) — prefixFor's
     * collision set sees only the base row, never the slot's
     * alias_-prefixed columns. */
    static String extraPrefix(String headKey, String alias,
            ClassSource cs) {
        return headKey.lastIndexOf('#') >= 0
                ? alias + "_"
                        + headKey.substring(headKey.lastIndexOf('#') + 1)
                        + "_"
                : AssociationJoins.prefixFor(headKey, cs);
    }

    /** Install each composited step's oriented hop-1 condition on the
     * parent pipeline (the mid rides INSIDE the identity's frame, never
     * at parent level). */
    static TypedSpec applyOrientedConds(TypedSpec csPipe,
            Map<String, TypedLambda> compositeConds) {
        for (var ce : compositeConds.entrySet()) {
            TypedSpec rewritten = NavMaterializer.rewriteNavPredicate(
                    csPipe, ce.getKey(), ce.getValue());
            if (rewritten == csPipe) {
                throw new IllegalStateException("resolver bug: composited"
                        + " navigate step '" + ce.getKey() + "' is not on"
                        + " the materialization spine — the oriented"
                        + " condition was not installed");
            }
            csPipe = rewritten;
        }
        return csPipe;
    }
}
