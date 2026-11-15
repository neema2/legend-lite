package com.legend.lowering;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.compiler.element.TypedFunction;
import java.util.List;
/**
 * Aggregate-reducer dispatch: the RESOLVED overload of an agg-col's reduce
 * lambda ({@code y|$y->sum()}) &rarr; the SQL reducer name. Identity-keyed like
 * {@link Scalars}; catalog-driven registration; unregistered = loud error. The table itself is
 * {@link com.legend.compiler.element.Reducers} (C5-J item 3, 2026-11-15: below the resolver and the lowering both).
 */
public final class Aggregates {

    /** The signature keys this registry reduces — a CLAIM per key
     *  ({@link com.legend.builtin.Claims}, kind REDUCER). */
    static java.util.Set<com.legend.model.FunctionId> reducerKeys() {
        return com.legend.compiler.element.Reducers.keys();
    }

    private Aggregates() {
    }

    /**
     * SQL reducer for the resolved reduce overload; loud error when
     * unregistered. Takes the TYPED callee — the parser node never crosses
     * into the lowering (AUDIT_2026_07 §1c; also retires the redundant
     * second parameter, audit L7).
     */
    /** Nullable variant of {@link #reducerFor} — for is-this-a-reducer probes. */
    static com.legend.sql.SqlAgg.@com.legend.base.Nullable Fn reducerOrNull(TypedFunction callee) {
        return com.legend.compiler.element.Reducers.aggregate(callee.id());
    }

    /** The ONE aggregate-membership test (remediation T1.7): resolver
     * walls ask the reducer catalog, never a parallel name list — a
     * catalog addition is a wall addition by construction. */
    static boolean isReducerKey(com.legend.model.FunctionId id) {
        return com.legend.compiler.element.Reducers.aggregate(id) != null;
    }

    static com.legend.sql.SqlAgg.Fn reducerFor(TypedFunction callee) {
        com.legend.sql.SqlAgg.Fn name = com.legend.compiler.element.Reducers.aggregate(callee.id());
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
