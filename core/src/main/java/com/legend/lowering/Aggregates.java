package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.compiler.element.TypedFunction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Aggregate-reducer dispatch: the RESOLVED overload of an agg-col's reduce
 * lambda ({@code y|$y->sum()}) &rarr; the SQL reducer name. Identity-keyed like
 * {@link Scalars}; catalog-driven registration; unregistered = loud error.
 */
public final class Aggregates {

    private static final Map<com.legend.model.FunctionId, SqlAgg.Fn> REDUCERS = new HashMap<>();

    /** The signature keys this registry reduces — a CLAIM per key
     *  ({@link com.legend.builtin.Claims}, kind REDUCER). */
    static java.util.Set<com.legend.model.FunctionId> reducerKeys() {
        return java.util.Collections.unmodifiableSet(REDUCERS.keySet());
    }

    private Aggregates() {
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
        for (com.legend.model.FunctionId f : com.legend.model.FunctionId.ofAll(Pure.AND__BOOLEAN_MANY)) {
            REDUCERS.put(f, SqlAgg.Fn.BOOL_AND);
        }
        for (com.legend.model.FunctionId f : com.legend.model.FunctionId.ofAll(Pure.OR__BOOLEAN_MANY)) {
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

    /**
     * SQL reducer for the resolved reduce overload; loud error when
     * unregistered. Takes the TYPED callee — the parser node never crosses
     * into the lowering (AUDIT_2026_07 §1c; also retires the redundant
     * second parameter, audit L7).
     */
    /** Nullable variant of {@link #reducerFor} — for is-this-a-reducer probes. */
    static com.legend.sql.SqlAgg.@com.legend.base.Nullable Fn reducerOrNull(TypedFunction callee) {
        return REDUCERS.get(callee.id());
    }

    /** The ONE aggregate-membership test (remediation T1.7): resolver
     * walls ask the reducer catalog, never a parallel name list — a
     * catalog addition is a wall addition by construction. */
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

    static boolean isReducerKey(com.legend.model.FunctionId id) {
        return REDUCERS.containsKey(id);
    }

    static com.legend.sql.SqlAgg.Fn reducerFor(TypedFunction callee) {
        com.legend.sql.SqlAgg.Fn name = REDUCERS.get(callee.id());
        if (name == null) {
            throw new IllegalStateException(
                    "no aggregate lowering registered for resolved overload '"
                            + callee.qualifiedName() + "'");
        }
        com.legend.builtin.DecisionProbe.pick(callee.definition(), "AGGREGATE");
        return name;
    }


    /** The reducer a percentile's (ascending, continuous) flags select,
     * plus whether the value's within-group order is DESCENDING. The
     * order is SEMANTIC (SQL-standard PERCENTILE_x(p) WITHIN GROUP
     * (ORDER BY v DESC)): continuous descending interpolates in the
     * reverse direction (engine golden 1.4 over [1,1.5,2]); discrete
     * descending picks the ceil(p*N)-th largest. Dialects whose
     * quantile family takes no order (DuckDB) spell the direction
     * themselves. */
    record AggFlavor(SqlAgg.Fn fn, boolean descending) {
    }

    static AggFlavor aggFlavor(SqlAgg.Fn fn,
            List<Boolean> flags, int extras) {
        if (flags.isEmpty()) {
            return new AggFlavor(fn, false);
        }
        // variance(numbers, isBiasCorrected) / stdDev(numbers, isBiasCorrected):
        // the flag picks the SAMPLE (bias-corrected) or POPULATION estimator
        if (fn == SqlAgg.Fn.VAR_SAMP && flags.size() == 1 && extras == 0) {
            return new AggFlavor(flags.get(0)
                    ? SqlAgg.Fn.VAR_SAMP : SqlAgg.Fn.VAR_POP, false);
        }
        if (fn == SqlAgg.Fn.STDDEV_SAMP && flags.size() == 1 && extras == 0) {
            return new AggFlavor(flags.get(0)
                    ? SqlAgg.Fn.STDDEV_SAMP : SqlAgg.Fn.STDDEV_POP, false);
        }
        if (fn == SqlAgg.Fn.QUANTILE_CONT && flags.size() == 2
                && extras == 1) {
            if (flags.get(0)) {
                return new AggFlavor(flags.get(1)
                        ? SqlAgg.Fn.QUANTILE_CONT
                        : SqlAgg.Fn.QUANTILE_DISC, false);
            }
            return new AggFlavor(flags.get(1)
                    ? SqlAgg.Fn.QUANTILE_CONT
                    : SqlAgg.Fn.QUANTILE_DISC, true);
        }
        throw new IllegalStateException("boolean reducer arguments are"
                + " only understood on percentile(p, ascending,"
                + " continuous) and variance(isBiasCorrected)");
    }

}
