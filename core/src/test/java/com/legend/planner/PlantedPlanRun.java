// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * A class PLANTED in the planner's package that runs a plan, for one test alone (C's self-audit, check 6, 2026-11-12):
 * {@code ArchitectureTest.thePlannerRuleBitesAPlantedRun} shows {@code ArchitectureTest.plannerIsExecutionFree} fails
 * on it. A test class: the rule's real check reads the product's classes, never this one.
 */
public final class PlantedPlanRun {

    private PlantedPlanRun() {
    }

    /** A plan's rows read while planning: the defect the rule refuses. */
    public static com.legend.exec.PlanRunner.Rows ran(com.legend.executionplan.ExecutionPlan plan) throws Exception {
        return com.legend.exec.PlanRunner.rows(plan, java.util.Map.of(), com.legend.exec.PlanSessions.shared(),
                java.util.UUID.randomUUID());
    }
}
