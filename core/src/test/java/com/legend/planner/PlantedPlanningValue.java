// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * Classes PLANTED in the planner's package to make values of their own, for one test alone:
 * {@code ArchitectureTest.theNoValueRuleBitesAPlantedPlanningClock} shows
 * {@code ArchitectureTest.planningMakesNoValueOfItsOwn} fails on each. Test classes: the rule's real check reads the
 * product's classes, never these.
 */
public final class PlantedPlanningValue {

    private PlantedPlanningValue() {
    }

    /** An id planning would have made of its own: the defect the rule refuses. */
    public static java.util.UUID minted() {
        return java.util.UUID.randomUUID();
    }

    /** A planning class that reads the clock as a value. */
    public static final class Clock {

        private Clock() {
        }

        /** A time planning would have written into a plan. */
        public static long now() {
            return System.nanoTime();
        }
    }
}
