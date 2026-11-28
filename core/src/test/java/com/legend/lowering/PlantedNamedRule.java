// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

/**
 * A class PLANTED in the lowering's package that names one of the rules an edition's table may name, for one test
 * alone: {@code ArchitectureTest.theNamedRuleRuleBitesAPlantedNamer} shows {@code anEditionsTableAloneNamesARule}
 * fails on it. A test class: the rule's real check reads the product's classes, never this.
 */
public final class PlantedNamedRule {

    private PlantedNamedRule() {
    }

    /** The defect the rule refuses: the product choosing, by itself, a rule that only a door's table may choose. */
    public static NamedRule chosenByTheProduct() {
        return NamedRule.SUBSTRING_AS_GIVEN;
    }
}
