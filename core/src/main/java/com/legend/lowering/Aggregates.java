package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.TypedFunction;
import java.util.HashMap;
import java.util.Map;
/**
 * Aggregate-reducer dispatch: the RESOLVED overload of an agg-col's reduce
 * lambda ({@code y|$y->sum()}) &rarr; the SQL reducer name. Identity-keyed like
 * {@link Scalars}; catalog-driven registration; unregistered = loud error.
 */
final class Aggregates {

    private static final Map<String, String> REDUCERS = new HashMap<>();

    private Aggregates() {
    }

    private static void family(String sqlName, String pureName) {
        for (String f : Pure.nativeKeysAt(pureName)) {
            REDUCERS.put(f, sqlName);
        }
    }

    static {
        family("SUM", "sum");
        // Pure spells numeric reduction via plus: y|$y->plus() == sum.
        family("SUM", "plus");
        family("COUNT", "count");
        family("AVG", "average");
        family("MIN", "min");
        family("MAX", "max");
        family("STDDEV_SAMP", "stdDevSample");
        family("STDDEV_SAMP", "stdDev");
        family("COUNT", "size");
        // joinStrings carries its separator as an EXTRA reduce-call argument
        // (handled in the lowering's aggExpr).
        family("STRING_AGG", "joinStrings");
        family("STDDEV_POP", "stdDevPopulation");
        family("VAR_SAMP", "varianceSample");
        family("VAR_POP", "variancePopulation");
        // Pure's bare variance is the SAMPLE variance (PCT semantics).
        family("VAR_SAMP", "variance");
        family("MEDIAN", "median");
        family("AVG", "mean");
        family("MODE", "mode");
        // percentile: DuckDB QUANTILE family; the 4-arg overload's
        // ascending/continuous flags are folded in the lowering (aggExpr).
        family("QUANTILE_CONT", "percentile");
        family("QUANTILE_CONT", "percentileCont");
        family("QUANTILE_DISC", "percentileDisc");
        // BI-VARIATE reducers — the map body is rowMapper(a, b); aggExpr
        // decomposes it into the two SQL arguments.
        family("CORR", "corr");
        family("COVAR_SAMP", "covarSample");
        family("COVAR_POP", "covarPopulation");
        family("ARG_MAX", "maxBy");
        family("ARG_MIN", "minBy");
        // wavg has NO single SQL reducer: SUM(v*w)/SUM(w), composed in
        // aggExpr — the marker name never reaches the renderer.
        family("__WAVG__", "wavg");
        // hashCode of a GROUP is HASH(LIST(values)) — composed in aggValue.
        family("__HASH_LIST__", "hashCode");
    }

    /**
     * SQL reducer for the resolved reduce overload; loud error when
     * unregistered. Takes the TYPED callee — the parser node never crosses
     * into the lowering (AUDIT_2026_07 §1c; also retires the redundant
     * second parameter, audit L7).
     */
    /** Nullable variant of {@link #reducerFor} — for is-this-a-reducer probes. */
    static String reducerOrNull(TypedFunction callee) {
        return REDUCERS.get(callee.signatureKey());
    }

    static String reducerFor(TypedFunction callee) {
        String name = REDUCERS.get(callee.signatureKey());
        if (name == null) {
            throw new IllegalStateException(
                    "no aggregate lowering registered for resolved overload '"
                            + callee.qualifiedName() + "'");
        }
        return name;
    }
}
