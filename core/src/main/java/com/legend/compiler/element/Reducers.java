// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;

import com.legend.builtin.Pure;

/**
 * THE REDUCERS' MEMBERSHIP: which resolved overloads reduce a group (C5-J item 3, 2026-11-15: below both the resolver
 * and the lowering, so the resolver's demand scan reads it without reaching the lowering -- the resolver depends on the
 * lowering no more). Membership alone (the C5-J audit's nit, 2026-11-15): the aggregate each reduces to is the
 * lowering's binding table ({@code lowering.Aggregates}, invariant 2), whose keys are checked equal to these as it is
 * sealed. Keyed by the typed callee's id: the resolver asks {@link #isReducer} and {@link #isDemandReducer}. A catalog
 * addition is a wall addition by construction (remediation T1.7).
 */
public final class Reducers {

    /** Every overload that reduces a group. */
    private static final com.legend.base.Sealed.SealedSet<com.legend.model.FunctionId> KEYS =
            com.legend.base.Sealed.set(new java.util.HashSet<>());

    /** The reducers of the group ALONE (pure first(): the lowering's per-group witness, ANY_VALUE): reduce-lambda-only,
     *  never a demand reducer. */
    private static final com.legend.base.Sealed.SealedSet<com.legend.model.FunctionId> GROUP_ONLY =
            com.legend.base.Sealed.set(new java.util.HashSet<>());

    private Reducers() {
    }

    private static void members(java.util.List<com.legend.model.FunctionId> ids) {
        KEYS.addAll(ids);
    }

    static {
        members(Pure.AT_MATH_SUM);
        // Pure spells numeric reduction via plus: y|$y->plus() == sum.
        members(com.legend.model.FunctionId.all(Pure.AT_MATH_PLUS, Pure.AT_STRING_PLUS));
        members(Pure.AT_COLLECTION_COUNT);
        members(Pure.AT_MATH_AVERAGE);
        members(com.legend.model.FunctionId.all(Pure.AT_DATE_MIN, Pure.AT_MATH_MIN, Pure.AT_COLLECTION_MIN));
        members(com.legend.model.FunctionId.all(Pure.AT_DATE_MAX, Pure.AT_MATH_MAX, Pure.AT_COLLECTION_MAX));
        // H2-LENIENT per-group witness (view ~groupBy per-row columns —
        // the engine's H2 1.x golden spells the BARE column; our DB-side
        // form is ANY_VALUE): REAL pure first() — order-sensitive
        // first()-over-relation consumers keep their limit-1 route by
        // excluding ANY_VALUE at THEIR arms, never a synthetic native
        members(com.legend.model.FunctionId.all(Pure.AT_RELATION_FIRST, Pure.AT_COLLECTION_FIRST));
        GROUP_ONLY.addAll(com.legend.model.FunctionId.all(Pure.AT_RELATION_FIRST, Pure.AT_COLLECTION_FIRST));
        members(Pure.AT_MATH_STD_DEV_SAMPLE);
        // upstream's flagged stdDev(numbers, isBiasCorrected) — the flag picks
        // SAMP/POP in the lowering's aggFlavor, exactly as variance's does
        members(Pure.AT_MATH_STD_DEV);
        members(com.legend.model.FunctionId.all(Pure.AT_RELATION_SIZE, Pure.AT_COLLECTION_SIZE));
        // joinStrings carries its separator as an EXTRA reduce-call argument
        // (handled in the lowering's aggExpr).
        members(com.legend.model.FunctionId.all(Pure.AT_STRING_JOIN_STRINGS, Pure.AT_RELATION_JOIN_STRINGS));
        members(Pure.AT_MATH_STD_DEV_POPULATION);
        members(Pure.AT_MATH_VARIANCE_SAMPLE);
        members(Pure.AT_MATH_VARIANCE_POPULATION);
        // Pure's bare variance is the SAMPLE variance (PCT semantics).
        members(Pure.AT_MATH_VARIANCE);
        members(Pure.AT_MATH_MEDIAN);
        members(Pure.AT_MATH_MEAN);
        members(Pure.AT_MATH_MODE);
        // Boolean reductions: y|$y->and() / ->or() over a group — DuckDB
        // BOOL_AND/BOOL_OR (engine simpleGroupByAnd/Or goldens). The
        // 1-arg COLLECTION overloads only: the 2-arg logical and(a,b)
        // must never register as a reducer.
        for (com.legend.model.FunctionId f : Pure.AT_COLLECTION_AND) {
            KEYS.add(f);
        }
        // the group: or(Boolean[*]) and or(Boolean[1..*]) (build rebuild Phase 3)
        for (com.legend.model.FunctionId f : Pure.AT_COLLECTION_OR) {
            KEYS.add(f);
        }
        // percentile: DuckDB QUANTILE family; the 4-arg overload's
        // ascending/continuous flags are folded in the lowering (aggExpr).
        members(Pure.AT_MATH_PERCENTILE);
        // lite's ema(values, alpha) (the trader screens' price average, 2026-11-14): alpha rides as the reduce call's
        // extra argument, as percentile's does (the lowering's Aggregates binds it)
        members(Pure.AT_LEGEND_LITE_EMA);
        // BI-VARIATE reducers — the map body is rowMapper(a, b); aggExpr
        // decomposes it into the two SQL arguments.
        members(Pure.AT_MATH_CORR);
        members(Pure.AT_MATH_COVAR_SAMPLE);
        members(Pure.AT_MATH_COVAR_POPULATION);
        members(Pure.AT_MATH_MAX_BY);
        members(Pure.AT_MATH_MIN_BY);
        // lite's minBy and maxBy over a TEXT key (the Cypher island's orderability, W11): the same reducers
        members(Pure.AT_LEGEND_LITE_MAX_BY);
        members(Pure.AT_LEGEND_LITE_MIN_BY);
        // wavg has NO single SQL reducer: SUM(v*w)/SUM(w), composed in
        // aggExpr — the marker name never reaches the renderer.
        members(Pure.AT_MATH_WAVG);
        // hashCode of a GROUP is HASH(LIST(values)) — composed in aggValue.
        members(Pure.AT_HASH_HASH_CODE);
        // isDistinct of a GROUP is COUNT(DISTINCT x) = COUNT(x) — composed
        // in aggValue (engine testGroupByIsDistinct golden). EXACT overload
        // only (audit 22a M5): the legacy 2-arg isDistinct(l,r) must never
        // reach the marker — its args would be dropped and the group SQL
        // rendered for a constantly-true pure expression.
        for (com.legend.model.FunctionId f : com.legend.model.FunctionId.ofAll(Pure.IS_DISTINCT__T_MANY)) {
            KEYS.add(f);
        }
        // the unique group value or NULL (collectionExtension.pure
        // semantics over a group): composed CASE, no single SQL reducer
        for (com.legend.model.FunctionId f : com.legend.model.FunctionId.ofAll(Pure.UNIQUE_VALUE_ONLY__T_MANY)) {
            KEYS.add(f);
        }
        for (com.legend.model.FunctionId f : com.legend.model.FunctionId.ofAll(Pure.UNIQUE_VALUE_ONLY__T_MANY__T_01)) {
            KEYS.add(f);
        }
    }

    /** Every overload that reduces a group. */
    public static java.util.Set<com.legend.model.FunctionId> keys() {
        return java.util.Collections.unmodifiableSet(KEYS);
    }

    /** The reducers of the group alone ({@code first()}), never a demand reducer. */
    public static java.util.Set<com.legend.model.FunctionId> groupOnly() {
        return java.util.Collections.unmodifiableSet(GROUP_ONLY);
    }

    /** What {@code bound} -- a binding table's keys -- holds that these do not, and lacks that these hold: the drift a
     *  binding table's check names. */
    public static String differences(java.util.Set<com.legend.model.FunctionId> bound) {
        java.util.Set<com.legend.model.FunctionId> extra = new java.util.TreeSet<>(java.util.Comparator.comparing(
                Object::toString));
        extra.addAll(bound);
        extra.removeAll(KEYS);
        java.util.Set<com.legend.model.FunctionId> missing = new java.util.TreeSet<>(java.util.Comparator.comparing(
                Object::toString));
        missing.addAll(KEYS);
        missing.removeAll(bound);
        return "bound but no member " + extra + ", a member but unbound " + missing;
    }

    /** Whether the resolved overload reduces a group. */
    public static boolean isReducer(TypedFunction callee) {
        return KEYS.contains(callee.id());
    }

    /** DEMAND-scan membership: reducers that make an expression an
     * AGGREGATE-over-navigation. A group-only reducer (pure first(), the
     * lowering's ANY_VALUE) is a reduce-lambda-ONLY entry — first() over a
     * projected nav collection keeps its join-row route (engine
     * ParentVarReferenceWithProject golden: plain LEFT JOINs, no grouped
     * subselect). */
    public static boolean isDemandReducer(TypedFunction callee) {
        return isReducer(callee) && !GROUP_ONLY.contains(callee.id());
    }

    /** Node-level demand membership (§4AD decision 1): INFIX plus —
     * the parser's n-ary carrier {@code plus([a, b, …])}, a LITERAL RUN of
     * two or more operands — is a ROW-WISE operation over a mapped
     * navigation in the engine's algebra (witness
     * testQualifierWithOperation: concat per fanned row), never a
     * reduction; the collection form over a VALUE ({@code $ages->plus()} ==
     * sum) reduces. The and/or precedent, applied at the node by the
     * argument's SHAPE (upstream declares plus variadic only — batch 5 leg 5). */
    public static boolean isDemandReducer(TypedFunction callee,
            com.legend.compiler.spec.typed.TypedSpec argument) {
        return isDemandReducer(callee)
                && !(com.legend.compiler.element.type.PlatformTypes.isPlus(callee.qualifiedName())
                        && argument instanceof com.legend.compiler.spec.typed.TypedCollection run
                        && run.operatorRun());
    }

    // the membership is a constant of the process from here on (2026-11-09, base.Sealed: a write after this is refused)
    static {
        KEYS.seal();
        GROUP_ONLY.seal();
    }
}
