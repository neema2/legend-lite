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
 * @param judgingRules lite's own, or the compatibility rules (an edge implies them)
 * @param options      lite's own planning options for every query of the body ({@link com.legend.platform.PlanOption}:
 *                     the tests turn on {@code DETERMINISTIC_AS_OF})
 * @param spelling     where the body's statements take their dialects (phase D's D1.0: the product's own, or the
 *                     compatibility edge's)
 */
public record PlanningOptions(
        com.legend.testexecutionplan.@com.legend.base.Nullable Edge edge,
        com.legend.executionplan.DataStores stores,
        java.util.Set<com.legend.platform.Feature> features,
        JudgeMode judgeMode,
        com.legend.testexecutionplan.TestPlan.JudgingRules judgingRules,
        java.util.Set<com.legend.platform.PlanOption> options,
        com.legend.database.SqlSpelling spelling) {

    public PlanningOptions {
        java.util.Objects.requireNonNull(stores, "stores");
        java.util.Objects.requireNonNull(spelling, "spelling");
        features = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(features));
        // an enum set: its order the declaration's (NoSaltedIterationOrderTest)
        options = options.isEmpty() ? java.util.Set.of()
                : java.util.Collections.unmodifiableSet(java.util.EnumSet.copyOf(options));
        if (edge != null && judgingRules != com.legend.testexecutionplan.TestPlan.JudgingRules.COMPILED_PURE_COMPAT) {
            // the user's ruling of 2026-11-08 (docs/PLAN_AS_DATA_2026_11_07.md 3a.3): the edge, with the referee's
            // ledger, is the compatibility rules' -- lite's own rules plan nothing for it
            throw new IllegalArgumentException("a run handed the compatibility edge judges by the compatibility rules");
        }
    }

    /** Lite's own rules, or backwards compatibility's where {@code engineCompat} ({@code ExecuteOptions.withEngineCompat}:
     *  the judges' leniencies that meet legend-engine's goldens; off, lite judges exactly). */
    public static com.legend.testexecutionplan.TestPlan.JudgingRules rules(boolean engineCompat) {
        return engineCompat ? com.legend.testexecutionplan.TestPlan.JudgingRules.COMPILED_PURE_COMPAT
                : com.legend.testexecutionplan.TestPlan.JudgingRules.LITE;
    }

    /** These options under lite's own rules or backwards compatibility's ({@link #rules}). */
    public PlanningOptions withEngineCompat(boolean on) {
        return new PlanningOptions(edge, stores, features, judgeMode, rules(on), options, spelling);
    }

    /** The assert judge of a RUN (docs/JUDGING_TWO_MODES): HOST -- the verdict of record, judged in Java over each
     *  side's plan result; DATABASE -- both sides planned and the database returns the verdict row (VerdictSql, a plan
     *  whose one row is the verdict); BOTH -- the agreement gate (the plan path program's step 6, 2026-10-18): both
     *  judges, the host's verdict of record, a disagreement a failure naming both (AgreementArm). One mode per run,
     *  carried on the run's options -- never a JVM global. The one judge-mode enumeration (2026-11-09: the door's
     *  options kept a copy of it, mapped by a switch). */
    public enum JudgeMode { HOST, DATABASE, BOTH }

    public PlanningOptions withJudgeMode(JudgeMode mode) {
        return new PlanningOptions(edge, stores, features, mode, judgingRules, options, spelling);
    }

    public PlanningOptions withFeatures(java.util.Set<com.legend.platform.Feature> f) {
        return new PlanningOptions(edge, stores, f, judgeMode, judgingRules, options, spelling);
    }

    /** Lite's own planning options for every query of the body ({@link com.legend.platform.PlanOption}). */
    public PlanningOptions withOptions(java.util.Set<com.legend.platform.PlanOption> o) {
        return new PlanningOptions(edge, stores, features, judgeMode, judgingRules, o, spelling);
    }

    /** These options with the program's statements written in {@code d}'s dialects (the spelling hook, D1.0: the
     *  compatibility edge's, {@code com.legend.edge.PureText}, for PCT and the corpus). */
    public PlanningOptions withSpelling(com.legend.database.SqlSpelling d) {
        return new PlanningOptions(edge, stores, features, judgeMode, judgingRules, options, d);
    }
}
