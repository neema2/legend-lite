// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.diagnostics;

/**
 * H6 PROTOTYPE (claude/compat-judges-homework, MEASUREMENT ONLY, never to merge): where each of legend-pure's comparison
 * leniencies changes a judge's decision, and a switch that turns each off (the judge then answers lite's exact rule).
 * {@code -Dlegend.measure.leniency.log=1} prints one {@code [lenient]} line per changed decision, with the running test;
 * {@code -Dlegend.measure.leniency.off=<ids>|ALL} turns the named ones off. The ids are the register's: S15 (a scalar
 * Float within 2 ULP), S132 (a rendered text's Float cell within 2 ULP), S131 (an unsorted actual compared as a
 * multiset), S144 (json-simple 1.1.1's grammar, S366 with it), S137 (a relation counted as one value), S127 (an Integer
 * and a Decimal equal by their texts), S139 (a Decimal's scale part of its value).
 */
public final class LeniencyProbe {

    private LeniencyProbe() {
    }

    private static final @com.legend.base.Nullable String OFF = read("legend.measure.leniency.off");
    private static final boolean LOG = read("legend.measure.leniency.log") != null;

    private static @com.legend.base.Nullable String read(String key) {
        String v = System.getProperty(key);
        return v != null ? v : System.getenv(key.replace('.', '_').toUpperCase(java.util.Locale.ROOT));
    }

    /** Whether leniency {@code id} is turned off for this run. */
    public static boolean off(String id) {
        return OFF != null && (OFF.equals("ALL") || ("," + OFF + ",").contains("," + id + ","));
    }

    /** Leniency {@code id} changed a decision ({@code what}: a short description), whether or not it is off. */
    public static void changed(String id, String what) {
        if (LOG) {
            String w = what.replace('\n', ' ').replace('\t', ' ');
            System.out.println("[lenient]\t" + id + "\t" + System.getProperty("legend.test.current", "?") + "\t"
                    + (w.length() > 200 ? w.substring(0, 200) : w));
        }
    }
}
