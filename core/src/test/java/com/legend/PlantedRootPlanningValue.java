// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

/**
 * A class PLANTED in the root package to read the clock while it plans, for one test alone (the re-audit's note 6,
 * 2026-11-12): {@code ArchitectureTest.theNoValueRuleBitesAPlantedPlanningClock} shows the planning no-value rule, as
 * built for the root's plan-side classes ({@code Compiler}, {@code TypedQuery}, {@code HeldModels}) with this one named
 * beside them, fails on it and on its nested class. A test class: the rule's real check reads the product's classes.
 */
public final class PlantedRootPlanningValue {

    private PlantedRootPlanningValue() {
    }

    /** A plan stamped with the time it was made: the defect the rule refuses. */
    public static long planned() {
        return System.currentTimeMillis();
    }

    /** The same, in a nested class (a door's lambda or helper class is the door's). */
    public static final class Nested {
        private Nested() {
        }

        public static java.util.UUID planned() {
            return java.util.UUID.randomUUID();
        }
    }
}
