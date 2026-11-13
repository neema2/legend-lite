// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

/**
 * A class PLANTED in the lowering's package that makes a date literal of a text it was handed, unchecked, for one test
 * alone (the final audit, 2026-11-12): {@code ArchitectureTest.theTemporalLiteralRuleBitesAPlantedMaker} shows
 * {@code ArchitectureTest.onlyCheckedSitesMakeATemporalLiteral} fails on it. A test class: the rule's real check reads
 * the product's classes, never this one.
 */
public final class PlantedTemporalLiteral {

    private PlantedTemporalLiteral() {
    }

    /** {@code DATE '<text>'} of whatever text: the defect the rule refuses. */
    public static SqlExpr date(String text) {
        return new SqlExpr.DateLit(text);
    }
}
