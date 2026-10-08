// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.model.RelationalDataType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DRIFT GUARD (Pile-1 audit, 2026-08-18): the engine has TWO type-text
 * conventions and we spell each ONCE — {@code Ddl.dataTypeToSqlText}
 * (dataTypeToSqlText proper: {@code DECIMAL(p, s)} with a space) and
 * {@code PlanText.spell} (the plan resultColumns tuple:
 * {@code DECIMAL(p,s)} unspaced). Both are golden-pinned by different
 * passing corpus families. This test pins that the two tables agree on
 * EVERY shared arm modulo exactly that documented spacing delta — an
 * edit to one table that drifts anything else fails here.
 */
class TypeSpellingParityTest {

    @Test
    void theTwoEngineConventionsDifferOnlyByDecimalSpacing() {
        List<RelationalDataType> shared = List.of(
                new RelationalDataType.Integer_(),
                new RelationalDataType.BigInt(),
                new RelationalDataType.SmallInt(),
                new RelationalDataType.TinyInt(),
                new RelationalDataType.Varchar(200),
                new RelationalDataType.Char_(10),
                new RelationalDataType.Double_(),
                new RelationalDataType.Float_(),
                new RelationalDataType.Real(),
                new RelationalDataType.Decimal(18, 6),
                new RelationalDataType.Numeric(10, 2),
                new RelationalDataType.Timestamp(),
                new RelationalDataType.Date_(),
                new RelationalDataType.Bit());
        for (RelationalDataType t : shared) {
            assertEquals(
                    com.legend.setup.Ddl.dataTypeToSqlText(t)
                            .replace(", ", ","),
                    com.legend.plan.PlanText.spell(t),
                    "type-text conventions drifted beyond the documented"
                    + " decimal-spacing delta for " + t);
        }
    }
}
