// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

/**
 * A class PLANTED in the lowering's package that branches on a database's dialect, for one test alone:
 * {@code ArchitectureTest.theDialectBlindRuleBitesAPlantedBranch} shows {@code compileSideLayersAreDialectBlind} fails
 * on it. A test class: the rule's real check reads the product's classes, never this.
 */
public final class PlantedDialectBranch {

    private PlantedDialectBranch() {
    }

    /** The defect the rule refuses: the compiler deciding by the database. */
    public static boolean onDuckDb(com.legend.sql.dialect.SqlDialect dialect) {
        return dialect instanceof com.legend.sql.dialect.DuckDb;
    }
}
