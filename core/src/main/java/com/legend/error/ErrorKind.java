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
public enum ErrorKind implements ErrorCode {
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
    INSTANCE_NOT_OF("the value is not an instance of the requested type"),
    MATCH_FAILURE("no match arm accepts the value"),
    RANGE_STEP_ZERO("range step is zero"),
    CONSTRAINT_UNEVALUABLE("constraint not evaluable: data not available"),
    LIST_LENGTHS_DIFFER("the two lists differ in length"),
    MAP_KEYS_MISALIGNED("map keys do not align with values"),
    MAP_KEY_NULL("map key is null"),
    MAP_KEYS_NOT_UNIQUE("map keys are not unique"),
    TEXT_NOT_INTEGER("text is not an integer"),
    TEXT_NOT_FLOAT("text is not a float"),
    TEXT_NOT_DATE("text is not a date"),
    VALUE_NOT_HELD("the value is outside the range this database can hold"),
    FRACTION_NOT_KEPT("more fraction digits than this database keeps"),
    CSV_TOO_WIDE("CSV record wider than its table"),
    WINDOW_FRAME_INVERTED("window frame starts after it ends"),
    WINDOW_FRAME_OFFSET_INVALID("window frame offset is negative or empty"),
    SUBSTRING_OUT_OF_RANGE("substring range is out of the text's bounds"),
    /** {@code fail(message)}: the user's own message is the error's text; this word only where none was given. */
    USER_FAIL("fail"),
    /** A failure the database raised with no site of lite's (phase D's Phase 2, item 4, 2026-11-16; the user's ruling:
     *  "I'm good with database only giving the database message too"): its message is the database's own, verbatim,
     *  carried as the database's ({@link DataError#databaseMessage}), its SQLSTATE where the driver gives one. Not a
     *  raise kind: no statement writes it. */
    DATABASE_ERROR("the database refused the statement"),
    /** A session that could not be opened, or a connection lost (a driver's SQLSTATE class 08): the database's own
     *  message, as {@link #DATABASE_ERROR}'s. Not a raise kind. */
    CONNECTION_ERROR("the connection to the database failed"),
    /** A load's file larger than the load admits (J25 of the edge audit, 2026-11-17: {@code base.DataSize.TooLarge},
     *  its facts the file, its size and the limit): the run refuses it before any statement reads it. Not a raise kind:
     *  no statement writes it. */
    FILE_TOO_LARGE("the file is larger than the load admits");

    /** Whether the kind is the database's own failure ({@link #DATABASE_ERROR}, {@link #CONNECTION_ERROR}), never a
     *  raise a statement writes. */
    public boolean isDatabases() {
        return this == DATABASE_ERROR || this == CONNECTION_ERROR;
    }

    /** Whether a statement raises the kind ({@code sql.RaiseKind} names it): every kind but the database's own failures
     *  and a load's refused file ({@link #FILE_TOO_LARGE}, refused by the run before a statement). */
    public boolean isRaise() {
        return !isDatabases() && this != FILE_TOO_LARGE;
    }

    private final String sentence;

    ErrorKind(String sentence) {
        this.sentence = sentence;
    }

    /** Lite's own short sentence of the kind: no value in it. */
    public String sentence() {
        return sentence;
    }

    @Override
    public String family() {
        return "run";
    }

    /** Lite's sentence over {@code values}: the kind's, then ': ' and its first value, the place it names (a run's
     *  error's where), as the run words it; the kind's alone where there is none. */
    @Override
    public String sentence(java.util.List<String> values) {
        return values.isEmpty() ? sentence : sentence + ": " + values.get(0);
    }
}
