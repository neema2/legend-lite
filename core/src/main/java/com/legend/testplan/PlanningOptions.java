// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * WHAT A TEST BODY'S PLANNING READS of the caller's options (the plan-as-data design's slice 5, 2026-11-09): the
 * compatibility edge (its planning hook), the data stores a load's reference is read from for its digest (data, never
 * a function: docs/DATA_IN_THE_PLAN_2026_11_09.md L1, 2026-11-09), the feature flags, the judge, and the rules the asserts
 * are judged by. The door makes it from its own options ({@code ExecuteOptions.planning}); the run's options -- the
 * system connections -- are never planning's.
 *
 * @param spelling     the dialects the program's statements are written in (D1.0's spelling hook)
 * @param options      lite's own planning options for every query of the body ({@link com.legend.platform.PlanOption}:
 *                     the tests turn on {@code DETERMINISTIC_AS_OF})
 * @param style        where the body's statements take their dialects (phase D's D1.0: the product's own, or the
 *                     compatibility edge's)
 */
public record PlanningOptions(
        com.legend.testexecutionplan.Edge edge,
        com.legend.executionplan.DataStores stores,
        java.util.Set<com.legend.platform.Feature> features,
        JudgeMode judgeMode,
        java.util.Set<com.legend.platform.PlanOption> options,
        com.legend.database.PlanStyle style) {

    public PlanningOptions {
        java.util.Objects.requireNonNull(edge, "edge");
        java.util.Objects.requireNonNull(stores, "stores");
        java.util.Objects.requireNonNull(style, "style");
        features = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(features));
        // an enum set: its order the declaration's (NoSaltedIterationOrderTest)
        options = options.isEmpty() ? java.util.Set.of()
                : java.util.Collections.unmodifiableSet(java.util.EnumSet.copyOf(options));
    }

    /** The assert judge of a RUN (docs/JUDGING_TWO_MODES): HOST -- the verdict of record, judged in Java over each
     *  side's plan result; DATABASE -- both sides planned and the database returns the verdict row (VerdictSql, a plan
     *  whose one row is the verdict); BOTH -- the agreement gate (the plan path program's step 6, 2026-10-18): both
     *  judges, the host's verdict of record, a disagreement a failure naming both (AgreementArm). One mode per run,
     *  carried on the run's options -- never a JVM global. The one judge-mode enumeration (2026-11-09: the door's
     *  options kept a copy of it, mapped by a switch). */
    public enum JudgeMode { HOST, DATABASE, BOTH }

    public PlanningOptions withJudgeMode(JudgeMode mode) {
        return new PlanningOptions(edge, stores, features, mode, options, style);
    }

    public PlanningOptions withFeatures(java.util.Set<com.legend.platform.Feature> f) {
        return new PlanningOptions(edge, stores, f, judgeMode, options, style);
    }

    /** Lite's own planning options for every query of the body ({@link com.legend.platform.PlanOption}). */
    public PlanningOptions withOptions(java.util.Set<com.legend.platform.PlanOption> o) {
        return new PlanningOptions(edge, stores, features, judgeMode, o, style);
    }

    /** These options with the program's statements written in {@code d}'s dialects (the spelling hook, D1.0: the
     *  compatibility edge's, {@code com.legend.edge.PureText}, for PCT and the corpus). */
    /** These options for ONE TEST BODY'S PLANNING CALL (K10b of the edge audit, 2026-11-17): the style a fresh
     *  recorder of its raise sites' words where it is the compatibility edge's ({@link EngineTextGates#planningCall}),
     *  read once by the test door; the product's style as it is. Asked by the test planner at the call's start alone. */
    PlanningOptions forOnePlanningCall() {
        return new PlanningOptions(edge, stores, features, judgeMode, options, EngineTextGates.planningCall(style));
    }

    public PlanningOptions withStyle(com.legend.database.PlanStyle d) {
        return new PlanningOptions(edge, stores, features, judgeMode, options, d);
    }
}
