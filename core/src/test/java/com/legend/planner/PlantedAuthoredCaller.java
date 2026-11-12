// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * Classes PLANTED to plan a person's statement past the boundary, for one test alone (the third re-audit's SHOULD-FIX 5,
 * 2026-11-12): {@code ArchitectureTest.theAuthoredDoorRuleBitesAPlantedCaller} shows
 * {@code ArchitectureTest.theAuthoredDoorIsReachedFromTheBoundaryAlone} fails on each. Test classes: the rule's real
 * check reads the product's classes, never these.
 */
public final class PlantedAuthoredCaller {

    private PlantedAuthoredCaller() {
    }

    /** Calls the door. */
    public static final class Calls {
        private Calls() {
        }

        public static com.legend.executionplan.ExecutionPlan planned(com.legend.sql.dialect.SqlDialect dialect,
                com.legend.executionplan.DatabaseType type) {
            return Authored.plan(dialect, type, "SELECT 1");
        }
    }

    /** Names the door by a reference. */
    public static final class ByReference {
        private ByReference() {
        }

        /** The door's shape. */
        public interface Door {
            com.legend.executionplan.ExecutionPlan plan(com.legend.sql.dialect.SqlDialect dialect,
                    com.legend.executionplan.DatabaseType type, String text);
        }

        public static Door door() {
            return Authored::plan;
        }
    }
}
