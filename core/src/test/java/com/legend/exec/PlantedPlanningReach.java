// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * A class PLANTED in exec's package that reaches the planner, for one test alone (the third re-audit, 2026-11-12):
 * {@code ArchitectureTest.theWallRuleFindsAPlantedReach} shows {@code ArchitectureTest.execReachesNoPlanning} finds it.
 * A test class: the rule's real check reads the product's classes, never this one.
 */
public final class PlantedPlanningReach {

    private PlantedPlanningReach() {
    }

    /** A target the runtime planned for itself: the wall's defect. */
    public static com.legend.executionplan.ExecutionPlan.Target planned(
            com.legend.executionplan.ExecutionPlan.Database database, com.legend.sql.dialect.SqlDialect dialect) {
        return com.legend.planner.Targets.of(database, dialect);
    }
}
