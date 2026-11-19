// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.database.Databases;

import org.junit.jupiter.api.Test;

/**
 * A PIVOT'S REDUCTION IT CANNOT TAKE (one that composes several SQL reducers: a pivot's USING takes exactly one) is
 * refused naming that reduction (the S374 probe's follow-up, 2026-11-19): until then the words named wavg whatever was
 * refused, and hashCode's hash of a list read "composes multiple reducers (wavg)".
 */
class PivotReductionWordsTest {

    @Test
    void aReductionThePivotCannotTakeIsRefusedByItsOwnName() {
        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> Compiler.query(Compiler.compileModel(PlanMakerTest.model(Databases.PLATFORM.name())),
                        "|#TDS\n city, year, w\n NYC, 2011, 1\n#->select(~[city, year, w])"
                                + "->pivot(~[year], ~['h' : x | $x.w : y | $y->hashCode()])")
                        .executionPlan("s::RT"));
        assertTrue(refused.getMessage().contains("aggregate 'h' reduces by meta::pure::functions::hash::hashCode")
                && !refused.getMessage().contains("wavg"), refused.getMessage());
    }
}
