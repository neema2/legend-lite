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
 *   <li>{@code corpus-trace}, {@code detach-trace}, {@code progress}, {@code timing}: the corpus runner's traces</li>
 *   <li>{@code corpus-containing=TEXT}: parser-equivalence's corpus, only the sources containing TEXT (iteration)</li>
 *   <li>{@code chb-only=TEXT}: Channel B, only the PCT tests whose name contains TEXT (iteration; no pins)</li>
 *   <li>{@code plan-sizes}: record, for every plan the round-trip gate judges, its JSON's size with and without the
 *       typed tree and its statements' characters (docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §8, 2026-10-18)</li>
 *   <li>{@code sql-sizes}: print each statement's characters and the running test's name before it is sent (the core
 *       program's size measurement, 2026-11-03)</li>
 *   <li>{@code judge-declines}: print each assert the database judge declines or leaves UNJUDGED, with the running
 *       test's name (the core program's 100% check, 2026-11-04: a decline under BOTH is the host's to decide, so a
 *       green run alone does not show the database judge decided every assert)</li>
 *   <li>{@code capture-queries=FILE}: append each product query planned from text -- its model, its query and its
 *       runtime's name -- to FILE as a JSON line ({@link QueryCapture}; E3, 2026-11-12): the lean-SQL baseline's
 *       sets</li>
 *   <li>{@code body-class}: print the assert program's class rule that refused a body, by statement (the walk
 *       deletion's measurement, 2026-11-06)</li>
 * </ul>
 */
public final class Diagnostics {

    private Diagnostics() {}

    /** The vocabulary: the switches that exist. */
    public static final List<String> NAMES = List.of(
            "dump-sql", "pct-cases", "pct-shapes", "corpus-trace", "detach-trace", "progress", "timing",
            // ("primitives" deleted 2026-11-09 with its census, PrimitiveCensus, which had no feeder: a dead switch)
            "corpus-containing", "chb-only", "plan-sizes",
            // the core program's SQL size (2026-11-03, the user's "core test SQL much smaller than PCT/corpus"): each
            // statement sent, its characters and the test that sent it, before it is sent
            "sql-sizes",
            // the core program's 100% check (2026-11-04): each database-judge decline or UNJUDGED, with its test
            "judge-declines",
            // the walk deletion's measurement (2026-11-06): the assert program's class rule that refused a body
            // (testplan/BodyClass, which reads the switch by name: the test-planning library depends on no switchboard)
            "body-class",
            // the plan-as-data design (2026-11-07): each test body's plan printed as its JSON (TestPlanner)
            "plan-json",
            // guard 3 in report mode (the plan-as-data design's step 3, 2026-11-08): every test body run on its plan
            // READ BACK from its JSON, never the planner's objects (TestPlanner)
            "decoded-plan",
            // the lean-SQL baseline's capture (E3, 2026-11-12): each product query planned from text, its model, query
            // and runtime NAME appended to the file the switch names (QueryCapture)
            "capture-queries");

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

    /** {@code judge-declines}: one line per database-judge decline or UNJUDGED, with the running test (the core
     *  program's 100% check, 2026-11-04). Measurement only: prints, decides nothing. */
    public static void judgeDecline(String kind, String name, String reason) {
        if (on("judge-declines")) {
            System.err.println("[judge-decline] " + kind + " " + System.getProperty("legend.test.current", "?") + " "
                    + name + ": " + reason.replace('\n', ' '));
        }
    }

    /** {@code judge-declines}, its second half: one line per statement a verdict sends AFTER the plan's rows are read
     *  -- a split rung, a ruled fallback, an appeal, a caller's message, a verdict statement run with no batch -- and
     *  per host step the database judge's plan holds (the strict database judge's measurement, 2026-11-05: after the
     *  plan decides, the harness may only explain a failure, never change the verdict). Measurement only. */
    public static void judgeSend(String kind, String name) {
        if (on("judge-declines")) {
            System.err.println("[judge-send] " + kind + " " + System.getProperty("legend.test.current", "?") + " "
                    + name);
        }
    }

    /** {@code dump-sql}; the LEGEND_LITE_DUMP_SQL env flag still turns it on until P7-14 retires the env flags. */
    public static boolean dumpSql() {
        return on("dump-sql") || System.getenv("LEGEND_LITE_DUMP_SQL") != null;
    }
}
