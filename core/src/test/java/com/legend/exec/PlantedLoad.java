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

    /** A load's step named by its constructor's reference, never called (2026-11-13, the orchestrator's completion B:
     *  {@code callConstructorWhere} saw calls alone). */
    public static final class ByReference {
        private ByReference() {
        }

        public static java.util.function.Function<String, ExecutionPlan.LoadStep> maker() {
            return ExecutionPlan.LoadStep.Statement::new;
        }
    }
}
