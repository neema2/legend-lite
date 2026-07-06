package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.parser.element.Function;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Aggregate-reducer dispatch: the RESOLVED overload of an agg-col's reduce
 * lambda ({@code y|$y->sum()}) &rarr; the SQL reducer name. Identity-keyed like
 * {@link Scalars}; catalog-driven registration; unregistered = loud error.
 */
final class Aggregates {

    private static final Map<Function, String> REDUCERS = new IdentityHashMap<>();

    private Aggregates() {
    }

    private static void family(String sqlName, String pureName) {
        for (Function f : Pure.nativeFunctionsAt(pureName)) {
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
    }

    /** SQL reducer for the resolved reduce overload; loud error when unregistered. */
    static String reducerFor(Function definition, String qualifiedName) {
        String name = REDUCERS.get(definition);
        if (name == null) {
            throw new IllegalStateException(
                    "no aggregate lowering registered for resolved overload '" + qualifiedName + "'");
        }
        return name;
    }
}
