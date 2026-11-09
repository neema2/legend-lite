// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.testexecutionplan.RootLoop;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The verdict row's column order, as the run reads it by position ({@link RootLoop}'s column constants), is the order
 *  the verdict statement selects ({@link VerdictSql#schema}): the one contract between the two halves (slice 5,
 *  2026-11-09: the run names no compiler type, so it reads the row by position). */
class VerdictRowOrderTest {

    @Test
    void theRunsPositionsAreTheSchemasOrder() {
        List<String> names = VerdictSql.schema().columns().stream()
                .map(com.legend.compiler.element.type.Type.RelationType.Column::name).toList();
        assertEquals(VerdictSql.VERDICT, names.get(RootLoop.VERDICT_COLUMN));
        assertEquals(VerdictSql.EXPECTED, names.get(RootLoop.EXPECTED_COLUMN));
        assertEquals(VerdictSql.ACTUAL, names.get(RootLoop.ACTUAL_COLUMN));
        assertEquals(VerdictSql.UNJUDGED, names.get(RootLoop.UNJUDGED_COLUMN));
        assertEquals(VerdictSql.LENIENT, names.get(RootLoop.LENIENT_COLUMN));
        assertEquals(VerdictSql.HOLDS, names.get(RootLoop.HOLDS_COLUMN));
        assertEquals(VerdictSql.MESSAGE, names.get(RootLoop.MESSAGE_COLUMN));
        assertEquals(7, names.size());
    }
}
