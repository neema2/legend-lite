// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AssertSentenceTest {

    @Test
    void aRowFillsItsSlotsInOrder() {
        assertEquals("expected 3, got 4", AssertSentence.EQUALS.fill(Map.of("expected", "3", "actual", "4")));
        assertEquals("expected [1, 2] not to contain 2",
                AssertSentence.NOT_CONTAINS.fill(Map.of("collection", "[1, 2]", "value", "2")));
        assertEquals("expected at least one value, got none", AssertSentence.NOT_EMPTY.fill(Map.of()));
    }

    @Test
    void aMissingOrExtraSlotIsRefusedByName() {
        assertThrows(IllegalArgumentException.class, () -> AssertSentence.EQUALS.fill(Map.of("expected", "3")));
        assertThrows(IllegalArgumentException.class,
                () -> AssertSentence.TRUE.fill(Map.of("expected", "3")));
    }

    @Test
    void everyRowParses() {
        for (AssertSentence s : AssertSentence.values()) {
            StringBuilder back = new StringBuilder();
            for (AssertSentence.Piece p : s.pieces()) {
                back.append(switch (p) {
                    case AssertSentence.Text t -> t.text();
                    case AssertSentence.Slot slot -> "{" + slot.name() + "}";
                });
            }
            assertEquals(s.template(), back.toString(), s.name());
        }
    }
}
