// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * A class PLANTED in exec's package to mint an id, for one test alone: {@code ArchitectureTest
 * .theNoValueRuleBitesAPlantedRandomUuid} shows {@code ArchitectureTest.execMakesNoValueOfItsOwn} fails on it. A test
 * class: the rule's real check reads the product's classes, never this one.
 */
public final class PlantedRandomUuid {

    private PlantedRandomUuid() {
    }

    /** An id exec would have made of its own: the defect the rule refuses. */
    public static java.util.UUID minted() {
        return java.util.UUID.randomUUID();
    }
}
