// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * A class PLANTED in exec's package to read a load's data itself, for one test alone (the final audit, 2026-11-12):
 * {@code ArchitectureTest.theDataRuleBitesAPlantedRead} shows {@code ArchitectureTest.execParsesNoData} fails on it. A
 * test class: the rule's real check reads the product's classes, never this one.
 */
public final class PlantedDataRead {

    private PlantedDataRead() {
    }

    /** A CSV's blocks read by exec: the defect the rule refuses (the database reads a load's data). */
    public static int blocks(String csv) {
        return com.legend.setup.CsvSeed.blocks(csv).size();
    }
}
