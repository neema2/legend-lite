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
 *   <li>{@code pct-shapes}: record each PCT case's result shape -- its kind, Pure type and value classes (the MongoDB
 *       line's homework, 2026-10-17: which shapes a lane's result route must carry)</li>
 *   <li>{@code pct-shadow-plan}: beside each PCT case's result, plan the same expression to Pure's TDS text
 *       ({@code TypedQuery.Output.TDS_TEXT}), run it by the plan runner and record whether the two agree (the MongoDB
 *       line's homework 2, 2026-10-17)</li>
 *   <li>{@code corpus-trace}, {@code detach-trace}, {@code progress}, {@code timing}: the corpus runner's traces</li>
 *   <li>{@code corpus-containing=TEXT}: parser-equivalence's corpus, only the sources containing TEXT (iteration)</li>
 *   <li>{@code chb-only=TEXT}: Channel B, only the PCT tests whose name contains TEXT (iteration; no pins)</li>
 *   <li>{@code primitives}: tally the MIR primitives of every statement the executor sends, by outcome, printed at
 *       the JVM's exit (the coverage map's input, exec/PrimitiveCensus)</li>
 *   <li>{@code plan-sizes}: record, for every plan the round-trip gate judges, its JSON's size with and without the
 *       typed tree and its statements' characters (docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §8, 2026-10-18)</li>
 * </ul>
 */
public final class Diagnostics {

    private Diagnostics() {}

    /** The vocabulary: the switches that exist. */
    public static final List<String> NAMES = List.of(
            "dump-sql", "pct-cases", "pct-shapes", "pct-shadow-plan", "corpus-trace", "detach-trace", "progress", "timing",
            "corpus-containing", "chb-only", "primitives", "plan-sizes");

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
