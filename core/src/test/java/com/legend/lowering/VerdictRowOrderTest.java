// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.testexecutionplan.RootLoop;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A typed verdict row's verdict, as the run reads it by position ({@link RootLoop#VERDICT_COLUMN}), is the column the
 *  verdict statement selects first: the one contract between the two halves (slice 5, 2026-11-09: the run names no
 *  compiler type, so it reads the row by position; item 3's S4: every database row is typed, the seven-column row and
 *  its sentence gone). */
class VerdictRowOrderTest {

    @Test
    void theRunsVerdictIsATypedRowsFirstColumn() {
        assertEquals(0, RootLoop.VERDICT_COLUMN);
        assertEquals(VerdictSql.VERDICT, VerdictSql.errorSchema().columns().get(RootLoop.VERDICT_COLUMN).name());
    }
}
