// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

/** The nearest-name hint the validator's messages carry (§6.3): an edit distance, case-insensitive. */
final class Names {

    private Names() {
    }

    static int distance(String a, String b) {
        String x = a.toLowerCase(java.util.Locale.ROOT);
        String y = b.toLowerCase(java.util.Locale.ROOT);
        int[] prev = new int[y.length() + 1];
        int[] cur = new int[y.length() + 1];
        for (int j = 0; j <= y.length(); j++) {
            prev[j] = j;
        }
        for (int i = 1; i <= x.length(); i++) {
            cur[0] = i;
            for (int j = 1; j <= y.length(); j++) {
                int cost = x.charAt(i - 1) == y.charAt(j - 1) ? 0 : 1;
                cur[j] = Math.min(Math.min(cur[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
            }
            int[] t = prev;
            prev = cur;
            cur = t;
        }
        return prev[y.length()];
    }

    /** The closest of {@code candidates} to {@code name} within two edits, or null. */
    static @com.legend.base.Nullable String nearest(Iterable<String> candidates, String name) {
        String best = null;
        int bestD = 3;
        for (String c : candidates) {
            int d = distance(c, name);
            if (d < bestD) {
                bestD = d;
                best = c;
            }
        }
        return best;
    }
}
