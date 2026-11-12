// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * Classes PLANTED in the planner's package for two tests alone (phase D's D1.0, 2026-11-12): a dialect outside the
 * product's dialect package and the edge ({@code ArchitectureTest.theDialectRuleBitesAPlantedDialect}), and a product
 * class calling a door that takes a dialect source ({@code ArchitectureTest.theSpellingDoorRuleBitesAPlantedCaller}).
 * Test classes: the rules' real checks read the product's classes, never these.
 */
public final class PlantedDialect extends com.legend.sql.dialect.DuckDb {

    /** A product class spelling a plan through a source of its own: the defect the rule refuses. */
    public static final class Spells {

        private Spells() {
        }

        public static com.legend.executionplan.ExecutionPlan plan(com.legend.TypedQuery typed) {
            return typed.executionPlan(null, java.util.Set.of(), com.legend.database.Databases::dialect);
        }
    }
}
