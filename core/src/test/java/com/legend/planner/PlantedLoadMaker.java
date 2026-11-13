// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;

/**
 * A class PLANTED in the planner's package that makes a load and one of its steps outside their makers, for one test
 * alone (the final audit, 2026-11-12): {@code ArchitectureTest.theLoadMakersRulesBiteAPlantedMaker} shows
 * {@code ArchitectureTest.onlyTheLoadPlannerMakesALoad} and {@code onlyThePlannersMakersMakeALoadStep} fail on it. A
 * test class: the rules' real checks read the product's classes, never this one.
 */
public final class PlantedLoadMaker {

    private PlantedLoadMaker() {
    }

    /** A load made outside the load planner, its step outside the step makers: the defects the rules refuse. */
    public static ExecutionPlan.Load made(ExecutionPlan.Target target) {
        return new ExecutionPlan.Load("load:planted", ExecutionPlan.Load.Cadence.BEFORE_STEP,
                java.util.List.of(new ExecutionPlan.LoadStep.Statement("SELECT 1")), target, java.util.Map.of());
    }
}
