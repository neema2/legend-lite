// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testplan.PlanningOptions;

/**
 * The execute OPTIONS a caller passes with one test body's execution -- they ride the request, never a static slot (the
 * PctRenderOption thread-local died here, batch 122). Two parts, one per half (the full audit, 2026-11-09: the options
 * held both halves' fields and mapped them, a switch and a check inside the test door's library): what planning reads
 * ({@link PlanningOptions}: the edge, the resource resolver, the feature flags, the judge, the judging rules) and what
 * the run reads (the host's system connections). A record of the two and its withers, no logic
 * ({@code JavaEvalLedgerTest.theDoorsHoldNoLogic}).
 *
 * @param planning          what the body's planning reads
 * @param systemConnections the host's system connections, by engine: a query reading the system metamodel store on a
 *                          server engine runs on that engine's (SystemDatabase); an in-process engine needs none
 */
public record ExecuteOptions(
        PlanningOptions planning,
        java.util.Map<com.legend.executionplan.DatabaseType, com.legend.exec.Sessions.Opener> systemConnections) {

    public ExecuteOptions {
        systemConnections = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(systemConnections));
    }

    /** Options with {@code edge} (the compatibility edge the run is handed, with the referee's raw-SQL ledger; null =
     *  none: lite's own rules -- an edge implies the compatibility rules, the plan-as-data design's step 8),
     *  {@code resources} (the test-input resource resolver, a resource's path to its local file; null = none) and
     *  {@code features}, judged on the host, with no system connection. */
    public ExecuteOptions(
            com.legend.testexecutionplan.@com.legend.base.Nullable Edge edge,
            java.util.function.@com.legend.base.Nullable Function<String, java.nio.file.Path> resources,
            java.util.Set<com.legend.platform.Feature> features) {
        this(new PlanningOptions(edge, resources, features, PlanningOptions.JudgeMode.HOST,
                PlanningOptions.rules(edge != null)), java.util.Map.of());
    }

    public static final ExecuteOptions NONE = new ExecuteOptions(null, null, java.util.Set.of());

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
        return new ExecuteOptions(planning.withJudgeMode(mode), systemConnections);
    }

    /** The runner's DEFAULT feature flags -- merged into every execute call's own ({@code ExecutionContext.features});
     *  the one ambient source (the engine's testable runner has the same knob). */
    public ExecuteOptions withFeatures(java.util.Set<com.legend.platform.Feature> f) {
        return new ExecuteOptions(planning.withFeatures(f), systemConnections);
    }

    /** The host's system connections, by engine: where a server engine's system database lives, configured apart from
     *  every runtime's connection (the platform holds no server credential; SystemDatabase). */
    public ExecuteOptions withSystemConnections(java.util.Map<com.legend.executionplan.DatabaseType,
            com.legend.exec.Sessions.Opener> connections) {
        return new ExecuteOptions(planning, connections);
    }

    /** BACKWARDS-COMPATIBILITY MODE (the user, 2026-11-04: "I want ours to be super clean, but be able to pass
     *  PCT/rcorpus in backwards compat mode"): the judges' leniencies that exist only to meet legend-engine's goldens --
     *  computed in Java doubles where lite computes in the database's -- apply in a run that asks for them (the PCT and
     *  corpus harnesses); lite's own runs judge exactly, as upstream's language does. The choice is made upfront and
     *  stated in the plan: every verdict step names its rules, which both judges read. Today: S132's float tokens within
     *  2 ULP inside a String, S127's Integer equal to a Decimal by text (testexec.Equality, testplan.DatabaseJudge). */
    public ExecuteOptions withEngineCompat(boolean on) {
        return new ExecuteOptions(planning.withEngineCompat(on), systemConnections);
    }

    /** The raw-SQL ledger this execution appends to (Phase 2b) and the test-input resource resolver its CSV loads read
     *  through (Phase 2d; a resource's path to its local file, loud when absent -- the reference checkout stays spec,
     *  never runtime): the caller owns both, the executor keeps neither. */
    public static ExecuteOptions recording(com.legend.testexecutionplan.Edge edge,
            java.util.function.@com.legend.base.Nullable Function<String, java.nio.file.Path> resources) {
        return new ExecuteOptions(edge, resources, java.util.Set.of());
    }
}
