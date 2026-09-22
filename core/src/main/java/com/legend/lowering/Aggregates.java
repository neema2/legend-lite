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

    private static final Map<String, SqlAgg.Fn> REDUCERS = new HashMap<>();

    /** The signature keys this registry reduces — a CLAIM per key
     *  ({@link com.legend.builtin.Claims}, kind REDUCER). */
    static java.util.Set<String> reducerKeys() {
        return java.util.Collections.unmodifiableSet(REDUCERS.keySet());
    }

    private Aggregates() {
    }

    private static void family(SqlAgg.Fn sqlName, String pureName) {
        for (String f : Pure.nativeKeysAt(pureName)) {
            REDUCERS.put(f, sqlName);
        }
    }

    static {
        family(SqlAgg.Fn.SUM, "sum");
        // Pure spells numeric reduction via plus: y|$y->plus() == sum.
        family(SqlAgg.Fn.SUM, "plus");
        family(SqlAgg.Fn.COUNT, "count");
        family(SqlAgg.Fn.AVG, "average");
        family(SqlAgg.Fn.MIN, "min");
        family(SqlAgg.Fn.MAX, "max");
        // H2-LENIENT per-group witness (view ~groupBy per-row columns —
        // the engine's H2 1.x golden spells the BARE column; our DB-side
        // form is ANY_VALUE): REAL pure first() — order-sensitive
        // first()-over-relation consumers keep their limit-1 route by
        // excluding ANY_VALUE at THEIR arms, never a synthetic native
        family(SqlAgg.Fn.ANY_VALUE, "first");
        family(SqlAgg.Fn.STDDEV_SAMP, "stdDevSample");
        // upstream's flagged stdDev(numbers, isBiasCorrected) — the flag picks
        // SAMP/POP in the lowering's aggFlavor, exactly as variance's does
        family(SqlAgg.Fn.STDDEV_SAMP, "stdDev");
        family(SqlAgg.Fn.COUNT, "size");
        // joinStrings carries its separator as an EXTRA reduce-call argument
        // (handled in the lowering's aggExpr).
        family(SqlAgg.Fn.STRING_AGG, "joinStrings");
        family(SqlAgg.Fn.STDDEV_POP, "stdDevPopulation");
        family(SqlAgg.Fn.VAR_SAMP, "varianceSample");
        family(SqlAgg.Fn.VAR_POP, "variancePopulation");
        // Pure's bare variance is the SAMPLE variance (PCT semantics).
        family(SqlAgg.Fn.VAR_SAMP, "variance");
        family(SqlAgg.Fn.MEDIAN, "median");
        family(SqlAgg.Fn.AVG, "mean");
        family(SqlAgg.Fn.MODE, "mode");
        // Boolean reductions: y|$y->and() / ->or() over a group — DuckDB
        // BOOL_AND/BOOL_OR (engine simpleGroupByAnd/Or goldens). The
        // 1-arg COLLECTION overloads only: the 2-arg logical and(a,b)
        // must never register as a reducer.
        for (String f : Pure.nativeKeysAt("and", 1)) {
            REDUCERS.put(f, SqlAgg.Fn.BOOL_AND);
        }
        for (String f : Pure.nativeKeysAt("or", 1)) {
            REDUCERS.put(f, SqlAgg.Fn.BOOL_OR);
        }
        // percentile: DuckDB QUANTILE family; the 4-arg overload's
        // ascending/continuous flags are folded in the lowering (aggExpr).
        family(SqlAgg.Fn.QUANTILE_CONT, "percentile");
        // BI-VARIATE reducers — the map body is rowMapper(a, b); aggExpr
        // decomposes it into the two SQL arguments.
        family(SqlAgg.Fn.CORR, "corr");
        family(SqlAgg.Fn.COVAR_SAMP, "covarSample");
        family(SqlAgg.Fn.COVAR_POP, "covarPopulation");
        family(SqlAgg.Fn.ARG_MAX, "maxBy");
        family(SqlAgg.Fn.ARG_MIN, "minBy");
        // wavg has NO single SQL reducer: SUM(v*w)/SUM(w), composed in
        // aggExpr — the marker name never reaches the renderer.
        family(SqlAgg.Fn.WAVG, "wavg");
        // hashCode of a GROUP is HASH(LIST(values)) — composed in aggValue.
        family(SqlAgg.Fn.HASH_LIST, "hashCode");
        // isDistinct of a GROUP is COUNT(DISTINCT x) = COUNT(x) — composed
        // in aggValue (engine testGroupByIsDistinct golden). EXACT overload
        // only (audit 22a M5): the legacy 2-arg isDistinct(l,r) must never
        // reach the marker — its args would be dropped and the group SQL
        // rendered for a constantly-true pure expression.
        for (String f : Pure.nativeKeysAt("isDistinct", 1)) {
            REDUCERS.put(f, SqlAgg.Fn.IS_DISTINCT_MARK);
        }
        // the unique group value or NULL (collectionExtension.pure
        // semantics over a group): composed CASE, no single SQL reducer
        for (String f : Pure.nativeKeysAt("uniqueValueOnly", 1)) {
            REDUCERS.put(f, SqlAgg.Fn.UNIQUE_VALUE_ONLY);
        }
        for (String f : Pure.nativeKeysAt("uniqueValueOnly", 2)) {
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
        return REDUCERS.get(callee.signatureKey());
    }

    /** The ONE aggregate-membership test (remediation T1.7): resolver
     * walls ask the reducer catalog, never a parallel name list — a
     * catalog addition is a wall addition by construction. */
    public static boolean isReducer(TypedFunction callee) {
        return REDUCERS.containsKey(callee.signatureKey());
    }

    /** DEMAND-scan membership: reducers that make an expression an
     * AGGREGATE-over-navigation. ANY_VALUE (pure first()) is a
     * reduce-lambda-ONLY entry — first() over a projected nav collection
     * keeps its join-row route (engine ParentVarReferenceWithProject
     * golden: plain LEFT JOINs, no grouped subselect). */
    public static boolean isDemandReducer(TypedFunction callee) {
        return isReducer(callee)
                && REDUCERS.get(callee.signatureKey()) != SqlAgg.Fn.ANY_VALUE;
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

    static boolean isReducerKey(String signatureKey) {
        return REDUCERS.containsKey(signatureKey);
    }

    static com.legend.sql.SqlAgg.Fn reducerFor(TypedFunction callee) {
        com.legend.sql.SqlAgg.Fn name = REDUCERS.get(callee.signatureKey());
        if (name == null) {
            throw new IllegalStateException(
                    "no aggregate lowering registered for resolved overload '"
                            + callee.qualifiedName() + "'");
        }
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
