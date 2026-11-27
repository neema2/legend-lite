// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * A class PLANTED in the planner's package that asks the door's edition a question, for one test alone:
 * {@code ArchitectureTest.theEditionReadRuleBitesAPlantedReader} shows
 * {@code thePlannersReadsOfTheEditionAreItsListedOnes} fails on it. A test class: the rule's real check reads the
 * product's classes, never this.
 */
public final class PlantedEditionReader {

    private PlantedEditionReader() {
    }

    /** The defect the rule refuses: a planning class, on no list, deciding by who is asking. */
    public static boolean decidesByTheDoor(Edition edition) {
        return edition.stampsTestBodies();
    }
}
