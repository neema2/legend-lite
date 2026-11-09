// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;


import com.legend.testexecutionplan.TestPlan;

/**
 * The execute OPTIONS a caller passes with one execution — they ride the
 * request and the result, never a static slot (the PctRenderOption
 * thread-local died here, batch 122).
 *
 * @param edge      the compatibility edge the run is handed, with the referee's raw-SQL ledger (null = none: lite's own
 *                  rules; an edge implies the compatibility rules, the plan-as-data design's step 8)
 * @param resources the test-input resource resolver (a resource's path → its local file; null = none): a CSV load's
 *                  statement names the file, which the database reads (2026-11-06)
 * @param systemConnections the host's system connections, by engine: a query reading the system metamodel store on a
 *                  server engine runs on that engine's (SystemDatabase); an in-process engine needs none
 * @param engineCompat backwards-compatibility mode ({@link #withEngineCompat}): the judges' leniencies that meet
 *                  legend-engine's goldens; off, lite judges exactly
 */
public record ExecuteOptions(
        com.legend.testexecutionplan.@com.legend.base.Nullable Edge edge,
        java.util.function.@com.legend.base.Nullable Function<String, java.nio.file.Path> resources,
        java.util.Set<com.legend.platform.Feature> features,
        JudgeMode judgeMode,
        java.util.Map<com.legend.model.ConnectionDefinition.DatabaseType, com.legend.exec.Sessions.Opener>
                systemConnections,
        boolean engineCompat) {

    public ExecuteOptions {
        systemConnections = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(systemConnections));
        if (edge != null && !engineCompat) {
            // the user's ruling of 2026-11-08 (docs/PLAN_AS_DATA_2026_11_07.md 3a.3): the edge, with the referee's
            // ledger, is the compatibility rules' -- lite's own rules plan nothing for it
            throw new IllegalArgumentException("a run handed the compatibility edge judges by the compatibility rules");
        }
    }

    /** The assert judge of a RUN (docs/JUDGING_TWO_MODES): HOST — the
     * verdict of record, judged in Java over each side's plan result; DATABASE —
     * both sides planned and the database returns the verdict row
     * (VerdictSql, a plan whose one row is the verdict); BOTH — the agreement
     * gate (the plan path program's step 6, 2026-10-18): both judges, the host's
     * verdict of record, a disagreement a failure naming both (AgreementArm). One
     * mode per run, carried on the run's options — never a JVM global (the lean
     * ladder and a corpus lane are different runs in one JVM). */
    public enum JudgeMode { HOST, DATABASE, BOTH }

    public ExecuteOptions(
            com.legend.testexecutionplan.@com.legend.base.Nullable Edge edge,
            java.util.function.@com.legend.base.Nullable Function<String, java.nio.file.Path> resources,
            java.util.Set<com.legend.platform.Feature> features) {
        this(edge, resources, features, JudgeMode.HOST, java.util.Map.of(), edge != null);
    }

    public static final ExecuteOptions NONE = new ExecuteOptions(null, null, java.util.Set.of());

    /** What a test body's planning reads of these options (slice 5, 2026-11-09: planning reads no run option). */
    public com.legend.testplan.PlanningOptions planning() {
        return new com.legend.testplan.PlanningOptions(edge, resources, features, switch (judgeMode) {
            case HOST -> com.legend.testplan.PlanningOptions.JudgeMode.HOST;
            case DATABASE -> com.legend.testplan.PlanningOptions.JudgeMode.DATABASE;
            case BOTH -> com.legend.testplan.PlanningOptions.JudgeMode.BOTH;
        }, judgingRules());
    }

    public ExecuteOptions withJudgeMode(JudgeMode mode) {
        return new ExecuteOptions(edge, resources, features, mode, systemConnections, engineCompat);
    }

    /** The runner's DEFAULT feature flags — merged into every execute
     *  call's own ({@code ExecutionContext.features}); the one ambient
     *  source (the engine's testable runner has the same knob). */
    public ExecuteOptions withFeatures(java.util.Set<com.legend.platform.Feature> f) {
        return new ExecuteOptions(edge, resources, java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(f)), judgeMode, systemConnections, engineCompat);
    }

    /** The host's system connections, by engine: where a server engine's system database lives, configured apart from
     *  every runtime's connection (the platform holds no server credential; SystemDatabase). */
    public ExecuteOptions withSystemConnections(java.util.Map<com.legend.model.ConnectionDefinition.DatabaseType,
            com.legend.exec.Sessions.Opener> connections) {
        return new ExecuteOptions(edge, resources, features, judgeMode, connections, engineCompat);
    }

    /** BACKWARDS-COMPATIBILITY MODE (the user, 2026-11-04: "I want ours to be super clean, but be able to pass
     *  PCT/rcorpus in backwards compat mode"): the judges' leniencies that exist only to meet legend-engine's goldens --
     *  computed in Java doubles where lite computes in the database's -- apply in a run that asks for them (the PCT and
     *  corpus harnesses); lite's own runs judge exactly, as upstream's language does. The choice is made upfront and
     *  stated in the plan: every verdict step names its rules ({@link #judgingRules}, TestPlan.JudgingRules), which
     *  both judges read. Today: S132's float tokens within 2 ULP inside a String, S127's Integer equal to a Decimal by
     *  text (judge.Equality, driver.DatabaseJudge). */
    public ExecuteOptions withEngineCompat(boolean on) {
        return new ExecuteOptions(edge, resources, features, judgeMode, systemConnections, on);
    }

    /** The rules this run's asserts are judged by, which its planners stamp on every verdict step (the user,
     *  2026-11-04: "explicit rule"): backwards compatibility's where the run asked for it, lite's own otherwise. */
    public com.legend.testexecutionplan.TestPlan.JudgingRules judgingRules() {
        return engineCompat ? com.legend.testexecutionplan.TestPlan.JudgingRules.COMPILED_PURE_COMPAT : com.legend.testexecutionplan.TestPlan.JudgingRules.LITE;
    }

    /** The raw-SQL ledger this execution appends to (Phase 2b) and the
     * test-input resource resolver its CSV loads read through (Phase 2d;
     * a resource's path → its local file, loud when absent — the reference checkout stays spec,
     * never runtime): the caller owns both, the executor keeps neither. */
    public static ExecuteOptions recording(com.legend.testexecutionplan.Edge edge,
            java.util.function.@com.legend.base.Nullable Function<String, java.nio.file.Path> resources) {
        return new ExecuteOptions(edge, resources, java.util.Set.of());
    }
}
