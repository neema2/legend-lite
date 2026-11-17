// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The product's Float text in Java ({@link CellText#productFloat}), register S311's rule as each dialect writes it. */
class ProductFloatTextTest {

    @Test
    void sixDecimalsTrimmed() {
        assertEquals("1.5", CellText.productFloat(1.5));
        assertEquals("2", CellText.productFloat(2.0));
        assertEquals("0.333333", CellText.productFloat(1.0 / 3));
        assertEquals("0.666667", CellText.productFloat(2.0 / 3));
        assertEquals("100000000000000000000", CellText.productFloat(1e20));
        assertEquals("0", CellText.productFloat(1e-9));
        assertEquals("0", CellText.productFloat(-0.0));
        assertEquals("-3.25", CellText.productFloat(-3.25));
    }

    @Test
    void theNonFiniteByName() {
        assertEquals("NaN", CellText.productFloat(Double.NaN));
        assertEquals("Infinity", CellText.productFloat(Double.POSITIVE_INFINITY));
        assertEquals("-Infinity", CellText.productFloat(Double.NEGATIVE_INFINITY));
    }
}
