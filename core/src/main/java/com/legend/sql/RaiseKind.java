// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * THE KINDS OF A PRODUCT RAISE (phase D's D1.1; the user's ruling of 2026-11-10, docs/DATA_DELIVERY_DESIGN_2026_11_10.md
 * §6.1: "a product error is its kind plus where it happened: a fixed text known at planning, never the value"): what a
 * statement's guard refuses, one closed list. A raise's message is a {@link SqlExpr.RaiseText} of one of these; the
 * product writes the kind's name alone, the compatibility edge legend-pure's sentence from the kind and its operands.
 * The product's own wording of each is exec's ({@code exec.RaisedErrors}), never SQL.
 */
public enum RaiseKind {
    DIVIDE_BY_ZERO,
    SHIFT_OUT_OF_RANGE,
    DATE_PART_ABSENT,
    INDEX_OUT_OF_BOUNDS,
    INSERT_OUT_OF_BOUNDS,
    SQRT_OF_NEGATIVE,
    SIZE_NOT_MULTIPLICITY,
    SLICE_BOUNDS_INVERTED,
    DATE_COMPONENT_INVALID,
    CAST_IMPOSSIBLE,
    RANGE_STEP_ZERO,
    CONSTRAINT_UNEVALUABLE,
    LIST_LENGTHS_DIFFER,
    MAP_KEYS_MISALIGNED,
    MAP_KEY_NULL,
    MAP_KEYS_NOT_UNIQUE,
    TEXT_NOT_INTEGER,
    TEXT_NOT_DATE,
    VALUE_NOT_HELD,
    FRACTION_NOT_KEPT,
    CSV_TOO_WIDE,
    /** {@code fail(message)}: the user's own message, the one operand the product writes. */
    USER_FAIL
}
