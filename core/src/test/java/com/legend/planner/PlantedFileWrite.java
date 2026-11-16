// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * A class PLANTED in the planner's package that writes a file, for one test alone (2026-11-15, PR1's review):
 * {@code ArchitectureTest.thePlannerFileRuleBitesAPlantedWrite} shows {@code ArchitectureTest.thePlannerWritesNoFile}
 * fails on it. A test class: the rule's real check reads the product's classes, never this one.
 */
public final class PlantedFileWrite {

    private PlantedFileWrite() {
    }

    /** A plan written to a file while planning, twice over: the defects the rule refuses. */
    public static void wrote(String text) throws Exception {
        java.nio.file.Files.writeString(java.nio.file.Path.of("plan.txt"), text);
        try (java.io.PrintWriter w = new java.io.PrintWriter(new java.io.File("plan2.txt"),
                java.nio.charset.StandardCharsets.UTF_8)) {
            w.print(text);
        }
    }
}
