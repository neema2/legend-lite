// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.base;

import java.util.Locale;
import java.util.Set;

/**
 * H5 SPIKE, TASK 0 (scratch, never merges): which of the edge's MEANING strands a measurement switches to the product's
 * meaning ({@code -Dh5.off=<strand>,...} or the {@code H5_OFF} environment variable). Read once per process. Strands:
 * {@code s20} (the bare-DECIMAL Float literal), {@code plain} (pure/v1's unchecked raises), {@code isnumeric},
 * {@code parseddecimal}, {@code parsedjson}, {@code sqlitedecimal} (SQLite's Decimal arithmetic as REAL), {@code raw}
 * (legend-engine's H2 executeInDb text translated for DuckDB and Postgres), {@code nullunsafe} (the Feature flag
 * LEGACY_SQL_NULL_UNSAFE_EQUALS), {@code settings} (H2 MODE=LEGACY, DEFAULT_NULL_ORDERING=HIGH), {@code crosskind}
 * (compiled legend-pure's Integer/Decimal equality by text, the database judge's); {@code all} for every one.
 */
public final class H5Off {

    private static final Set<String> OFF = read();

    private H5Off() {
    }

    private static Set<String> read() {
        String p = System.getProperty("h5.off", "") + "," + String.valueOf(System.getenv().getOrDefault("H5_OFF", ""));
        java.util.Set<String> s = new java.util.HashSet<>();
        for (String x : p.split(",")) {
            if (!x.isBlank()) {
                s.add(x.trim().toLowerCase(Locale.ROOT));
            }
        }
        if (!s.isEmpty()) {
            System.err.println("[H5] meaning strands switched to the product's: " + s);
        }
        return Set.copyOf(s);
    }

    /** Whether {@code strand} is switched to the product's meaning. */
    public static boolean off(String strand) {
        return OFF.contains("all") || OFF.contains(strand);
    }
}
