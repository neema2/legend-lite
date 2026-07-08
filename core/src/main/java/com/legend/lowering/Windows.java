package com.legend.lowering;

import com.legend.builtin.Pure;

import java.util.Map;

/**
 * Window-native dispatch: the RESOLVED overload of a window-column body
 * ({@code $p->lag($r)}, {@code rank($p, $w, $r)}) &rarr; its SQL window
 * function and position kind. Identity-keyed like {@link Scalars};
 * unregistered = loud error. The kind decides the {@code SqlAgg} branch:
 * RANKING functions take no column argument; VALUE functions take the column
 * the wrapping property access names.
 */
final class Windows {

    enum Kind { RANKING, VALUE }

    record WindowFn(String sqlName, Kind kind) {
    }

    private static final Map<String, WindowFn> FNS = new java.util.HashMap<>();

    private Windows() {
    }

    private static void family(String sqlName, Kind kind, String pureName) {
        for (String f : Pure.nativeKeysAt(pureName)) {
            FNS.put(f, new WindowFn(sqlName, kind));
        }
    }

    /** SQL reducer names for the 4-arg colToAgg window aggregates. */
    private static final Map<String, String> AGGREGATES = new java.util.HashMap<>();

    static {
        family("ROW_NUMBER", Kind.RANKING, "rowNumber");
        family("RANK", Kind.RANKING, "rank");
        family("DENSE_RANK", Kind.RANKING, "denseRank");
        family("PERCENT_RANK", Kind.RANKING, "percentRank");
        family("CUME_DIST", Kind.RANKING, "cumulativeDistribution");
        family("NTILE", Kind.RANKING, "ntile");
        family("LAG", Kind.VALUE, "lag");
        family("LEAD", Kind.VALUE, "lead");
        family("FIRST_VALUE", Kind.VALUE, "first");
        family("LAST_VALUE", Kind.VALUE, "last");
        family("NTH_VALUE", Kind.VALUE, "nth");
        // The 4-arg colToAgg window AGGREGATES — real pure has exactly these
        // (average, stdDevPopulation; everything else windows via the
        // agg-col spelling). Keyed by the _Window-bearing overload only —
        // the REDUCER overloads of the same names are Aggregates' domain.
        aggregate("AVG", "average");
        aggregate("STDDEV_POP", "stdDevPopulation");
    }

    private static void aggregate(String sqlName, String pureName) {
        for (String key : Pure.nativeKeysAt(pureName)) {
            if (key.contains("_Window")) {
                AGGREGATES.put(key, sqlName);
            }
        }
    }

    /** The SQL reducer for a 4-arg window-aggregate callee, or null. */
    static String aggregate(com.legend.compiler.element.TypedFunction callee) {
        return AGGREGATES.get(callee.signatureKey());
    }

    /** The window fn for a resolved overload, or null when it is not a window native. */
    static WindowFn lookup(com.legend.compiler.element.TypedFunction callee) {
        return FNS.get(callee.signatureKey());
    }
}
