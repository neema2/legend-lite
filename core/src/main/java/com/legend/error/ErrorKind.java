// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * THE KIND OF A PRODUCT ERROR RAISED IN A STATEMENT, AND ITS SENTENCE (phase D's D1.1; the user's ruling of 2026-11-10:
 * "a product error is its kind plus where it happened ... never the value"; the wording ruled by the orchestrator
 * 2026-11-12): the error layer's own vocabulary, beside the typed error ({@link DataError#kind}) -- exec parses a raise
 * into its kind and decides nothing more; the caller or writer spells it from here. One name for each of the plan's
 * raise kinds ({@code sql.RaiseKind}, the MIR's bare enum: the two held equal by name, both ways, by
 * {@code ErrorKindTest}). legend-pure's and legend-engine's sentences are the compatibility edge's.
 */
public enum ErrorKind {
    DIVIDE_BY_ZERO("division by zero"),
    SHIFT_OUT_OF_RANGE("bit shift outside 0..62"),
    DATE_PART_ABSENT("the date has no such part: it is coarser"),
    INDEX_OUT_OF_BOUNDS("index out of bounds"),
    INSERT_OUT_OF_BOUNDS("insert position out of bounds"),
    SQRT_OF_NEGATIVE("square root of a negative number"),
    SIZE_NOT_MULTIPLICITY("collection size does not match the multiplicity"),
    SLICE_BOUNDS_INVERTED("slice start is after its end"),
    DATE_COMPONENT_INVALID("invalid date component"),
    CAST_IMPOSSIBLE("the value cannot be cast to the requested type"),
    RANGE_STEP_ZERO("range step is zero"),
    CONSTRAINT_UNEVALUABLE("constraint not evaluable: data not available"),
    LIST_LENGTHS_DIFFER("the two lists differ in length"),
    MAP_KEYS_MISALIGNED("map keys do not align with values"),
    MAP_KEY_NULL("map key is null"),
    MAP_KEYS_NOT_UNIQUE("map keys are not unique"),
    TEXT_NOT_INTEGER("text is not an integer"),
    TEXT_NOT_DATE("text is not a date"),
    VALUE_NOT_HELD("the value is outside the range this database can hold"),
    FRACTION_NOT_KEPT("more fraction digits than this database keeps"),
    CSV_TOO_WIDE("CSV record wider than its table"),
    /** {@code fail(message)}: the user's own message is the error's text; this word only where none was given. */
    USER_FAIL("fail");

    private final String sentence;

    ErrorKind(String sentence) {
        this.sentence = sentence;
    }

    /** Lite's own short sentence of the kind: no value in it. */
    public String sentence() {
        return sentence;
    }
}
