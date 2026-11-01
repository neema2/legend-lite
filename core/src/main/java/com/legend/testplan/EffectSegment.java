// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.error.DataError;

import java.util.OptionalInt;

/**
 * AN EFFECT SEGMENT's name and its failure (the test-is-the-product plan's M5; the M5 audit's B3): a test body's run of
 * consecutive effect statements is sent as one script, and a failure in it is named by the statements of the body it
 * holds and by the one the database names. One spelling for the walk ({@code BodyCompiler}, {@code BodyDriver}) and
 * the test door's program road ({@code BodyRouter}), so a moved body fails with the walk's message.
 */
public final class EffectSegment {

    private EffectSegment() {
    }

    /** The segment holding the body's statements {@code from} to {@code to} (1-based), as a failure names it. */
    public static String where(int from, int to) {
        return from == to ? "(statement " + from + ")" : "(statements " + from + "–" + to + ")";
    }

    /** {@code failure} named where it happened: the segment {@code where} of {@code statements} statements, and the
     *  failing one where the database names it ({@code failing}, 0-based), else the script applied as a unit. */
    public static DataError failed(DataError failure, String where, OptionalInt failing, int statements) {
        return new DataError(failure.getMessage() + " — in the effect segment " + where
                + (failing.isPresent() ? " (statement " + (failing.getAsInt() + 1) + " of " + statements + ")"
                        : " (" + statements + " statement(s), applied as a unit)"), failure);
    }
}
