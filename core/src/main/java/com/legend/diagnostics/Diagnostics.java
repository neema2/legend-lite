// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.diagnostics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE DIAGNOSTICS SWITCHBOARD (Bazel workplan P3-13, A5): every debug or measuring switch a run can turn on, in one
 * option, {@code -Dlegend.diagnostics=name[=value],...}, read once. A switch is named here or it does not exist: an
 * unknown name fails the run (a typo never silently turns nothing on). P7-14 moves the remaining env flags here and
 * passes the option in at the entry points (lowering may not reach this class: ArchitectureTest invariant 6h).
 *
 * <ul>
 *   <li>{@code dump-sql}: print every statement the executor sends (the execution census's input)</li>
 *   <li>{@code pct-cases}: record each PCT case's model and expression (the render census's input)</li>
 *   <li>{@code corpus-trace}, {@code detach-trace}, {@code progress}, {@code timing}: the corpus runner's traces</li>
 *   <li>{@code corpus-containing=TEXT}: parser-equivalence's corpus, only the sources containing TEXT (iteration)</li>
 *   <li>{@code chb-only=TEXT}: Channel B, only the PCT tests whose name contains TEXT (iteration; no pins)</li>
 * </ul>
 */
public final class Diagnostics {

    private Diagnostics() {}

    /** The vocabulary: the switches that exist. */
    public static final List<String> NAMES = List.of(
            "dump-sql", "pct-cases", "corpus-trace", "detach-trace", "progress", "timing",
            "corpus-containing", "chb-only");

    private static final Map<String, String> ON = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(parse(System.getProperty("legend.diagnostics", ""))));

    static Map<String, String> parse(String option) {
        Map<String, String> on = new LinkedHashMap<>();
        for (String part : option.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) {
                continue;
            }
            int eq = p.indexOf('=');
            String name = eq < 0 ? p : p.substring(0, eq);
            if (!NAMES.contains(name)) {
                throw new IllegalArgumentException("-Dlegend.diagnostics: no switch named '" + name + "' (the switches: "
                        + NAMES + ")");
            }
            on.put(name, eq < 0 ? "" : p.substring(eq + 1));
        }
        return on;
    }

    /** Whether the switch is on. */
    public static boolean on(String name) {
        return ON.containsKey(name);
    }

    /** The switch's value, or null when it is off. */
    public static @com.legend.base.Nullable String value(String name) {
        return ON.get(name);
    }

    /** {@code dump-sql}; the LEGEND_LITE_DUMP_SQL env flag still turns it on until P7-14 retires the env flags. */
    public static boolean dumpSql() {
        return on("dump-sql") || System.getenv("LEGEND_LITE_DUMP_SQL") != null;
    }
}
