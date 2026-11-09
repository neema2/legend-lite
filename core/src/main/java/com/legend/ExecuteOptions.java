// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testplan.PlanningOptions;

/**
 * The execute OPTIONS a caller passes with one test body's execution -- they ride the request, never a static slot (the
 * PctRenderOption thread-local died here, batch 122). Two parts, one per half (the full audit, 2026-11-09: the options
 * held both halves' fields and mapped them, a switch and a check inside the test door's library): what planning reads
 * ({@link PlanningOptions}: the edge, the data stores, the feature flags, the judge, the judging rules) and what the run
 * reads (the host's system connections, the data stores its loads resolve through). A record of the two and its
 * withers, no logic ({@code JavaEvalLedgerTest.theDoorsHoldNoLogic}). The data stores are each half's own copy of one
 * value: planning reads a referenced file for its digest, the run for its bytes (docs/DATA_IN_THE_PLAN_2026_11_09.md
 * L1, 2026-11-09).
 *
 * @param planning          what the body's planning reads
 * @param systemConnections the host's system connections, by engine: a query reading the system metamodel store on a
 *                          server engine runs on that engine's (SystemDatabase); an in-process engine needs none
 * @param stores            where the run's loads resolve their references (data)
 */
public record ExecuteOptions(
        PlanningOptions planning,
        java.util.Map<com.legend.executionplan.DatabaseType, com.legend.exec.Sessions.Opener> systemConnections,
        com.legend.executionplan.DataStores stores) {

    public ExecuteOptions {
        systemConnections = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(systemConnections));
    }

    /** Options with {@code edge} (the compatibility edge the run is handed, with the referee's raw-SQL ledger; null =
     *  none: lite's own rules -- an edge implies the compatibility rules, the plan-as-data design's step 8),
     *  {@code stores} (where a load's data reference resolves, for planning and for the run alike) and
     *  {@code features}, judged on the host, with no system connection. */
    public ExecuteOptions(
            com.legend.testexecutionplan.@com.legend.base.Nullable Edge edge,
            com.legend.executionplan.DataStores stores,
            java.util.Set<com.legend.platform.Feature> features) {
        this(new PlanningOptions(edge, stores, features, PlanningOptions.JudgeMode.HOST,
                PlanningOptions.rules(edge != null)), java.util.Map.of(), stores);
    }

    public static final ExecuteOptions NONE = new ExecuteOptions(null, com.legend.executionplan.DataStores.none(),
            java.util.Set.of());

    /** The compatibility edge the run is handed, or null (lite's own rules). */
    public com.legend.testexecutionplan.@com.legend.base.Nullable Edge edge() {
        return planning.edge();
    }

    /** The rules this run's asserts are judged by, which its planners stamp on every verdict step (the user,
     *  2026-11-04: "explicit rule"). */
    public com.legend.testexecutionplan.TestPlan.JudgingRules judgingRules() {
        return planning.judgingRules();
    }

    /** The judge the body's asserts are planned for. */
    public PlanningOptions.JudgeMode judgeMode() {
        return planning.judgeMode();
    }

    /** These options judged as {@code run}'s are: its judge mode (a caller above the door names no planning type). */
    public ExecuteOptions judgedAs(ExecuteOptions run) {
        return withJudgeMode(run.judgeMode());
    }

    public ExecuteOptions withJudgeMode(PlanningOptions.JudgeMode mode) {
        return new ExecuteOptions(planning.withJudgeMode(mode), systemConnections, stores);
    }

    /** The runner's DEFAULT feature flags -- merged into every execute call's own ({@code ExecutionContext.features});
     *  the one ambient source (the engine's testable runner has the same knob). */
    public ExecuteOptions withFeatures(java.util.Set<com.legend.platform.Feature> f) {
        return new ExecuteOptions(planning.withFeatures(f), systemConnections, stores);
    }

    /** The host's system connections, by engine: where a server engine's system database lives, configured apart from
     *  every runtime's connection (the platform holds no server credential; SystemDatabase). */
    public ExecuteOptions withSystemConnections(java.util.Map<com.legend.executionplan.DatabaseType,
            com.legend.exec.Sessions.Opener> connections) {
        return new ExecuteOptions(planning, connections, stores);
    }

    /** BACKWARDS-COMPATIBILITY MODE (the user, 2026-11-04: "I want ours to be super clean, but be able to pass
     *  PCT/rcorpus in backwards compat mode"): the judges' leniencies that exist only to meet legend-engine's goldens --
     *  computed in Java doubles where lite computes in the database's -- apply in a run that asks for them (the PCT and
     *  corpus harnesses); lite's own runs judge exactly, as upstream's language does. The choice is made upfront and
     *  stated in the plan: every verdict step names its rules, which both judges read. Today: S132's float tokens within
     *  2 ULP inside a String, S127's Integer equal to a Decimal by text (testexec.Equality, testplan.DatabaseJudge). */
    public ExecuteOptions withEngineCompat(boolean on) {
        return new ExecuteOptions(planning.withEngineCompat(on), systemConnections, stores);
    }

    /** The raw-SQL ledger this execution appends to (Phase 2b) and the data stores its loads resolve through
     *  (2026-11-09: a resource by its name under the store's root, its digest checked; the reference checkout stays
     *  spec, never runtime): the caller owns both, the executor keeps neither. */
    public static ExecuteOptions recording(com.legend.testexecutionplan.Edge edge,
            com.legend.executionplan.DataStores stores) {
        return new ExecuteOptions(edge, stores, java.util.Set.of());
    }
}
