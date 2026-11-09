// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * WHAT A TEST BODY'S PLANNING READS of the caller's options (the plan-as-data design's slice 5, 2026-11-09): the
 * compatibility edge (its planning hook), the resource resolver, the feature flags, the judge, and the rules the asserts
 * are judged by. The door makes it from its own options ({@code ExecuteOptions.planning}); the run's options -- the
 * system connections -- are never planning's.
 *
 * @param judgingRules lite's own, or the compatibility rules (an edge implies them)
 */
public record PlanningOptions(
        com.legend.testexecutionplan.@com.legend.base.Nullable Edge edge,
        java.util.function.@com.legend.base.Nullable Function<String, java.nio.file.Path> resources,
        java.util.Set<com.legend.platform.Feature> features,
        JudgeMode judgeMode,
        com.legend.testexecutionplan.TestPlan.JudgingRules judgingRules) {

    public PlanningOptions {
        features = java.util.Set.copyOf(features);
    }

    /** The judge the body's asserts are planned for: the host's, the database's, or both and their agreement. */
    public enum JudgeMode { HOST, DATABASE, BOTH }

    public PlanningOptions withJudgeMode(JudgeMode mode) {
        return new PlanningOptions(edge, resources, features, mode, judgingRules);
    }

    public PlanningOptions withFeatures(java.util.Set<com.legend.platform.Feature> f) {
        return new PlanningOptions(edge, resources, f, judgeMode, judgingRules);
    }
}
