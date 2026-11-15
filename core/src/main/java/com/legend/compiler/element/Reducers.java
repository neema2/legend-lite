// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;

import com.legend.builtin.Pure;
import com.legend.sql.SqlAgg;
import java.util.HashMap;

/**
 * THE REDUCERS: which resolved overloads reduce a group, and the aggregate each reduces to (C5-J item 3, 2026-11-15:
 * moved here from {@code lowering.Aggregates}, below both the resolver and the lowering, so the resolver's demand scan
 * reads the reducer catalog without reaching the lowering -- the resolver depends on the lowering no more). The table
 * is data, keyed by the typed callee's id: the lowering reads its aggregate ({@code Aggregates.reducerFor}), the
 * resolver its membership ({@link #isReducer}, {@link #isDemandReducer}). A catalog addition is a wall addition by
 * construction (remediation T1.7).
 */
public final class Reducers {

    private static final com.legend.base.Sealed.SealedMap<com.legend.model.FunctionId, SqlAgg.Fn> REDUCERS =
            com.legend.base.Sealed.map(new HashMap<>());

    private Reducers() {
    }

    private static void family(SqlAgg.Fn sqlName, java.util.List<com.legend.model.FunctionId> ids) {
        for (com.legend.model.FunctionId f : ids) {
            REDUCERS.put(f, sqlName);
        }
    }

    static {
        family(SqlAgg.Fn.SUM, Pure.AT_MATH_SUM);
        // Pure spells numeric reduction via plus: y|$y->plus() == sum.
        family(SqlAgg.Fn.SUM, com.legend.model.FunctionId.all(Pure.AT_MATH_PLUS, Pure.AT_STRING_PLUS));
        family(SqlAgg.Fn.COUNT, Pure.AT_COLLECTION_COUNT);
        family(SqlAgg.Fn.AVG, Pure.AT_MATH_AVERAGE);
        family(SqlAgg.Fn.MIN, com.legend.model.FunctionId.all(Pure.AT_DATE_MIN, Pure.AT_MATH_MIN, Pure.AT_COLLECTION_MIN));
        family(SqlAgg.Fn.MAX, com.legend.model.FunctionId.all(Pure.AT_DATE_MAX, Pure.AT_MATH_MAX, Pure.AT_COLLECTION_MAX));
        // H2-LENIENT per-group witness (view ~groupBy per-row columns —
        // the engine's H2 1.x golden spells the BARE column; our DB-side
        // form is ANY_VALUE): REAL pure first() — order-sensitive
        // first()-over-relation consumers keep their limit-1 route by
        // excluding ANY_VALUE at THEIR arms, never a synthetic native
        family(SqlAgg.Fn.ANY_VALUE, com.legend.model.FunctionId.all(Pure.AT_RELATION_FIRST, Pure.AT_COLLECTION_FIRST));
        family(SqlAgg.Fn.STDDEV_SAMP, Pure.AT_MATH_STD_DEV_SAMPLE);
        // upstream's flagged stdDev(numbers, isBiasCorrected) — the flag picks
        // SAMP/POP in the lowering's aggFlavor, exactly as variance's does
        family(SqlAgg.Fn.STDDEV_SAMP, Pure.AT_MATH_STD_DEV);
        family(SqlAgg.Fn.COUNT, com.legend.model.FunctionId.all(Pure.AT_RELATION_SIZE, Pure.AT_COLLECTION_SIZE));
        // joinStrings carries its separator as an EXTRA reduce-call argument
        // (handled in the lowering's aggExpr).
        family(SqlAgg.Fn.STRING_AGG, com.legend.model.FunctionId.all(Pure.AT_STRING_JOIN_STRINGS, Pure.AT_RELATION_JOIN_STRINGS));
        family(SqlAgg.Fn.STDDEV_POP, Pure.AT_MATH_STD_DEV_POPULATION);
        family(SqlAgg.Fn.VAR_SAMP, Pure.AT_MATH_VARIANCE_SAMPLE);
        family(SqlAgg.Fn.VAR_POP, Pure.AT_MATH_VARIANCE_POPULATION);
        // Pure's bare variance is the SAMPLE variance (PCT semantics).
        family(SqlAgg.Fn.VAR_SAMP, Pure.AT_MATH_VARIANCE);
        family(SqlAgg.Fn.MEDIAN, Pure.AT_MATH_MEDIAN);
        family(SqlAgg.Fn.AVG, Pure.AT_MATH_MEAN);
        family(SqlAgg.Fn.MODE, Pure.AT_MATH_MODE);
        // Boolean reductions: y|$y->and() / ->or() over a group — DuckDB
        // BOOL_AND/BOOL_OR (engine simpleGroupByAnd/Or goldens). The
        // 1-arg COLLECTION overloads only: the 2-arg logical and(a,b)
        // must never register as a reducer.
        for (com.legend.model.FunctionId f : Pure.AT_COLLECTION_AND) {
            REDUCERS.put(f, SqlAgg.Fn.BOOL_AND);
        }
        // the group: or(Boolean[*]) and or(Boolean[1..*]) (build rebuild Phase 3)
        for (com.legend.model.FunctionId f : Pure.AT_COLLECTION_OR) {
            REDUCERS.put(f, SqlAgg.Fn.BOOL_OR);
        }
        // percentile: DuckDB QUANTILE family; the 4-arg overload's
        // ascending/continuous flags are folded in the lowering (aggExpr).
        family(SqlAgg.Fn.QUANTILE_CONT, Pure.AT_MATH_PERCENTILE);
        // BI-VARIATE reducers — the map body is rowMapper(a, b); aggExpr
        // decomposes it into the two SQL arguments.
        family(SqlAgg.Fn.CORR, Pure.AT_MATH_CORR);
        family(SqlAgg.Fn.COVAR_SAMP, Pure.AT_MATH_COVAR_SAMPLE);
        family(SqlAgg.Fn.COVAR_POP, Pure.AT_MATH_COVAR_POPULATION);
        family(SqlAgg.Fn.ARG_MAX, Pure.AT_MATH_MAX_BY);
        family(SqlAgg.Fn.ARG_MIN, Pure.AT_MATH_MIN_BY);
        // lite's minBy and maxBy over a TEXT key (the Cypher island's orderability, W11): the same reducers
        family(SqlAgg.Fn.ARG_MAX, Pure.AT_LEGEND_LITE_MAX_BY);
        family(SqlAgg.Fn.ARG_MIN, Pure.AT_LEGEND_LITE_MIN_BY);
        // wavg has NO single SQL reducer: SUM(v*w)/SUM(w), composed in
        // aggExpr — the marker name never reaches the renderer.
        family(SqlAgg.Fn.WAVG, Pure.AT_MATH_WAVG);
        // hashCode of a GROUP is HASH(LIST(values)) — composed in aggValue.
        family(SqlAgg.Fn.HASH_LIST, Pure.AT_HASH_HASH_CODE);
        // isDistinct of a GROUP is COUNT(DISTINCT x) = COUNT(x) — composed
        // in aggValue (engine testGroupByIsDistinct golden). EXACT overload
        // only (audit 22a M5): the legacy 2-arg isDistinct(l,r) must never
        // reach the marker — its args would be dropped and the group SQL
        // rendered for a constantly-true pure expression.
        for (com.legend.model.FunctionId f : com.legend.model.FunctionId.ofAll(Pure.IS_DISTINCT__T_MANY)) {
            REDUCERS.put(f, SqlAgg.Fn.IS_DISTINCT_MARK);
        }
        // the unique group value or NULL (collectionExtension.pure
        // semantics over a group): composed CASE, no single SQL reducer
        for (com.legend.model.FunctionId f : com.legend.model.FunctionId.ofAll(Pure.UNIQUE_VALUE_ONLY__T_MANY)) {
            REDUCERS.put(f, SqlAgg.Fn.UNIQUE_VALUE_ONLY);
        }
        for (com.legend.model.FunctionId f : com.legend.model.FunctionId.ofAll(Pure.UNIQUE_VALUE_ONLY__T_MANY__T_01)) {
            REDUCERS.put(f, SqlAgg.Fn.UNIQUE_VALUE_ONLY);
        }
    }

    /** The signature keys the table reduces. */
    public static java.util.Set<com.legend.model.FunctionId> keys() {
        return java.util.Collections.unmodifiableSet(REDUCERS.keySet());
    }

    /** The aggregate {@code id} reduces to, or null where it reduces nothing. */
    public static SqlAgg.@com.legend.base.Nullable Fn aggregate(com.legend.model.FunctionId id) {
        return REDUCERS.get(id);
    }

    /** Whether the resolved overload reduces a group. */
    public static boolean isReducer(TypedFunction callee) {
        return REDUCERS.containsKey(callee.id());
    }

    /** DEMAND-scan membership: reducers that make an expression an
     * AGGREGATE-over-navigation. ANY_VALUE (pure first()) is a
     * reduce-lambda-ONLY entry — first() over a projected nav collection
     * keeps its join-row route (engine ParentVarReferenceWithProject
     * golden: plain LEFT JOINs, no grouped subselect). */
    public static boolean isDemandReducer(TypedFunction callee) {
        return isReducer(callee)
                && REDUCERS.get(callee.id()) != SqlAgg.Fn.ANY_VALUE;
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

    // the registry is a constant of the process from here on (2026-11-09, base.Sealed: a write after this is refused)
    static {
        REDUCERS.seal();
    }
}
