// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

/**
 * A class PLANTED in exec's package to make a load's step of its own, for one test alone (C's self-audit, check 6,
 * 2026-11-12): {@code ArchitectureTest.theLoadRuleBitesAPlantedLoad} shows {@code ArchitectureTest.execMakesNoLoad}
 * fails on it. A test class: the rule's real check reads the product's classes, never this one.
 */
public final class PlantedLoad {

    private PlantedLoad() {
    }

    /** Data exec would have put in itself: the defect the rule refuses. */
    public static ExecutionPlan.LoadStep made() {
        return new ExecutionPlan.LoadStep.Statement("INSERT INTO T VALUES ('exec wrote this')");
    }
}
