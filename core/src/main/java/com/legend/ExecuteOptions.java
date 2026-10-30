// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

/**
 * The execute OPTIONS a caller passes with one execution — they ride the
 * request and the result, never a static slot (the PctRenderOption
 * thread-local died here, batch 122).
 *
 * @param recorder  the raw-SQL ledger the executor appends to (null = none)
 * @param resources the test-input resource resolver (path → text; null = none)
 * @param systemConnections the host's system connections, by engine: a query reading the system metamodel store on a
 *                  server engine runs on that engine's (SystemDatabase); an in-process engine needs none
 */
public record ExecuteOptions(
        com.legend.sql.dialect.RawSqlBoundary.@com.legend.base.Nullable Recorder recorder,
        java.util.function.@com.legend.base.Nullable Function<String, String> resources,
        java.util.Set<com.legend.platform.Feature> features,
        JudgeMode judgeMode,
        java.util.Map<com.legend.model.ConnectionDefinition.DatabaseType, com.legend.exec.Sessions.Opener>
                systemConnections) {

    public ExecuteOptions {
        systemConnections = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(systemConnections));
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
            com.legend.sql.dialect.RawSqlBoundary.@com.legend.base.Nullable Recorder recorder,
            java.util.function.@com.legend.base.Nullable Function<String, String> resources,
            java.util.Set<com.legend.platform.Feature> features) {
        this(recorder, resources, features, JudgeMode.HOST, java.util.Map.of());
    }

    public static final ExecuteOptions NONE = new ExecuteOptions(null, null, java.util.Set.of());

    public ExecuteOptions withJudgeMode(JudgeMode mode) {
        return new ExecuteOptions(recorder, resources, features, mode, systemConnections);
    }

    /** The runner's DEFAULT feature flags — merged into every execute
     *  call's own ({@code ExecutionContext.features}); the one ambient
     *  source (the engine's testable runner has the same knob). */
    public ExecuteOptions withFeatures(java.util.Set<com.legend.platform.Feature> f) {
        return new ExecuteOptions(recorder, resources, java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(f)), judgeMode, systemConnections);
    }

    /** The host's system connections, by engine: where a server engine's system database lives, configured apart from
     *  every runtime's connection (the platform holds no server credential; SystemDatabase). */
    public ExecuteOptions withSystemConnections(java.util.Map<com.legend.model.ConnectionDefinition.DatabaseType,
            com.legend.exec.Sessions.Opener> connections) {
        return new ExecuteOptions(recorder, resources, features, judgeMode, connections);
    }

    /** The raw-SQL ledger this execution appends to (Phase 2b) and the
     * test-input resource resolver its CSV loads read through (Phase 2d;
     * path → text, loud when absent — the reference checkout stays spec,
     * never runtime): the caller owns both, the executor keeps neither. */
    public static ExecuteOptions recording(com.legend.sql.dialect.RawSqlBoundary.Recorder r,
            java.util.function.@com.legend.base.Nullable Function<String, String> resources) {
        return new ExecuteOptions(r, resources, java.util.Set.of());
    }
}
