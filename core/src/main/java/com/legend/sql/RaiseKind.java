// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * THE KINDS OF A PRODUCT RAISE (phase D's D1.1; the user's ruling of 2026-11-10, docs/DATA_DELIVERY_DESIGN_2026_11_10.md
 * §6.1: "a product error is its kind plus where it happened: a fixed text known at planning, never the value"): what a
 * statement's guard refuses, one closed list. A raise's message is a {@link SqlExpr.RaiseText} of one of these; the
 * product writes the kind's name behind its {@link #MARK}, and its where, the compatibility edge legend-pure's sentence
 * from the kind and its operands. The product's WORDING of each is the error layer's {@code com.legend.error.ErrorKind}
 * (its sentence, held equal by name, both ways, by {@code ErrorKind}'s test), applied where exec reads the raise back
 * ({@code exec.RaisedErrors}), never SQL.
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
    /** A value whose run-time type is not the one a chain step needs (the resolver's run-time dispatch; W5, 2026-11-16). */
    INSTANCE_NOT_OF,
    /** A value no arm of a match accepts (W7, 2026-11-16): the value's type, and the arms' types where the resolver's
     *  run-time dispatch read them. */
    MATCH_FAILURE,
    RANGE_STEP_ZERO,
    CONSTRAINT_UNEVALUABLE,
    LIST_LENGTHS_DIFFER,
    MAP_KEYS_MISALIGNED,
    MAP_KEY_NULL,
    MAP_KEYS_NOT_UNIQUE,
    TEXT_NOT_INTEGER,
    /** Text that is not a Float, read as one (parseFloat; Phase 8, 2026-11-16). */
    TEXT_NOT_FLOAT,
    TEXT_NOT_DATE,
    /** Lite's refusals of a value a database cannot hold (S138, S181, a load's width), each its own kind with its facts
     *  the operands (stage B's BE-P, 2026-11-21: until then VALUE_NOT_HELD, FRACTION_NOT_KEPT and CSV_TOO_WIDE, whose
     *  operand was the place's prose). */
    INVERSE_TRIG_NAN,
    PARSED_NAN_NOT_HELD,
    NOT_FINITE_NOT_HELD,
    DATE_PAST_YEARS,
    DATE_OUTSIDE_YEARS,
    INTEGER_PAST_64_BITS,
    MIXED_RANK_INEXACT,
    PARAMETER_FRACTION_NOT_KEPT,
    SECOND_FRACTION_NOT_KEPT,
    CSV_TOO_WIDE,
    /** A window frame whose computed start comes after its end (merged with #81's frame guard, 2026-11-14). */
    WINDOW_FRAME_INVERTED,
    /** A window frame's computed offset that is negative or empty (S170; merged with #81's frame guard,
     *  2026-11-14). */
    WINDOW_FRAME_OFFSET_INVALID,
    /** A {@code substring} whose start is below 0, whose end is past the text's length, or whose end comes before its
     *  start (A3 of the compatibility program, 2026-11-19; register S383): legend-pure raises there (Java's
     *  {@code String.substring}), and so does the product, in the database. Operands: the start, the end, the length. */
    SUBSTRING_OUT_OF_RANGE,
    /** {@code fail(message)}: the user's own message, the one operand the product writes. */
    USER_FAIL;

    /** U+001C (file separator): the mark a product raise's text opens with, before its kind's name, so a user's
     *  {@code fail} text spelled as a kind's name is never read as that kind (the D part 1 audit's nit and S4,
     *  2026-11-15; the second mark, before a where, went with the where, stage B's BE item 2, 2026-11-21). exec reads it
     *  ({@code exec.RaisedErrors}). */
    public static final char MARK = '\u001c';

    /** What a raise SITE's token opens with (phase D's Phase 2, raises as SITES, 2026-11-16): the {@link #MARK}, then
     *  {@code LG#}, then the site's number in its statement's table ({@code sql.dialect.RaiseSite}); the statement raises
     *  the token alone. The mark opens it so a user's {@code fail} text spelled {@code LG#1} is never read as a site
     *  (the D part 1 audit's S4: the mark exists for that). */
    public static final String TOKEN = MARK + "LG#";

    /** The text a statement raises for its site {@code n}: the mark, then {@code LG#<n>}. */
    public static String token(int n) {
        return TOKEN + n;
    }

    /** The site number {@code text} is the token of, or -1 where it is no token (the mark, {@code LG#} and digits,
     *  nothing else: a user's {@code LG#1}, unmarked, is no token). */
    public static int siteNumber(String text) {
        if (!text.startsWith(TOKEN) || text.length() == TOKEN.length() || text.length() > TOKEN.length() + 9) {
            return -1;
        }
        for (int i = TOKEN.length(); i < text.length(); i++) {
            if (text.charAt(i) < '0' || text.charAt(i) > '9') {
                return -1;
            }
        }
        int n = Integer.parseInt(text.substring(TOKEN.length()));
        return n < 1 ? -1 : n;
    }
}
