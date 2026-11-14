// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.exec.PlanRunner;
import com.legend.executionplan.ExecutionPlan;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A BRANCH'S CONDITION IS READ BY ITS COLUMN'S DECLARED TYPE (the C final audit's J10, 2026-11-12): the cell as its
 * column's decode read it, a Boolean -- a constant condition through the one decode of a constant
 * ({@code PlanRunner.constantRows}), a statement's through its column's -- and never a kind decided from text: a text
 * "true" is refused by name (until then it was parsed here).
 */
class BranchConditionTest {

    private static final List<ExecutionPlan.Column> BOOLEAN = List.of(new ExecutionPlan.Column("c", "Boolean",
            new ExecutionPlan.Multiplicity(1, 1), "BOOLEAN", null, null));

    @Test
    void aBooleanCellNamesTheSide_aTextOneIsRefusedByName() {
        assertTrue(TestRun.taken(new PlanRunner.Rows(BOOLEAN, List.of(List.of(Boolean.TRUE)))));
        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> TestRun.taken(new PlanRunner.Rows(BOOLEAN, List.of(List.of("true")))));
        assertEquals("a branch's condition answered a String (true): an if's condition is one Boolean",
                refused.getMessage());
    }

    @Test
    void aConstantConditionIsReadByItsDeclaredType() {
        assertEquals(List.of(List.of(Boolean.FALSE)), PlanRunner.constantRows(BOOLEAN, List.of(List.of("false"))));
        assertThrows(IllegalStateException.class, () -> PlanRunner.constantRows(BOOLEAN, List.of(List.of("yes"))));
    }
}
