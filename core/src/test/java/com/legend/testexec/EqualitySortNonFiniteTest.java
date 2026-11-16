// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The ledger audit's D5 (2026-11-16): assertSameElements' sort reads a Float as a decimal; a NaN or an infinity has
 *  none, and is refused by name (Unjudged), never a NumberFormatException out of the judge. */
class EqualitySortNonFiniteTest {

    @Test
    void aNonFiniteFloatIsRefusedByName() {
        for (double d : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            Equality.Unjudged u = assertThrows(Equality.Unjudged.class, () -> Equality.sorted(List.of(
                    new Equality.Typed(1.5d, "Float"), new Equality.Typed(d, "Float"))));
            assertTrue(u.getMessage().contains("non-finite Float"), u.getMessage());
        }
    }

    @Test
    void finiteNumbersStillSortByValue() {
        List<Equality.Typed> s = Equality.sorted(List.of(new Equality.Typed(2.5d, "Float"),
                new Equality.Typed(1L, "Integer")));
        assertEquals(1L, s.get(0).value());
    }
}
