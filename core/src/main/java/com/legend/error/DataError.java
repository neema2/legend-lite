// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * THE DATABASE-ERROR TRANSLATION (the exception seam, user directive
 * 2026-09-01): a real {@code java.sql.SQLException} raised by the
 * executing database, translated to the platform vocabulary AT THE
 * EXECUTOR BOUNDARY — the one seam that understands JDBC. Everything
 * above the boundary (orchestration, verdict arms, the harness)
 * classifies failures by PLATFORM types only; {@code java.sql} stops
 * appearing in their signatures, which is what lets the JDBC registers
 * genuinely shrink to the executor seam. The original
 * {@code SQLException} rides as the cause; {@code getMessage()} is the
 * database's own text (the raised-message provenance discipline —
 * RaisedErrors — is unchanged and runs before translation).
 */
public final class DataError extends RuntimeException {

    /** The product raise's kind (phase D's D1.1), or null: a database's own error, the edge's sentence, a user's
     *  {@code fail} message. */
    private final @com.legend.base.Nullable ErrorKind kind;

    public DataError(String message, Throwable cause) {
        super(message, cause);
        this.kind = null;
    }

    /** A product raise of {@code kind}: its message the kind's sentence, spelled here, the error layer's
     *  ({@link ErrorKind#sentence}). */
    public DataError(ErrorKind kind, Throwable cause) {
        this(kind, null, cause);
    }

    /** A product raise of {@code kind} placed by {@code where}, a fixed text known at planning (a parameter's name, the
     *  digits a database keeps): its message the kind's sentence, then the where (the D part 1 audit's S4,
     *  2026-11-15: the user's "its kind and where it happened"). */
    public DataError(ErrorKind kind, @com.legend.base.Nullable String where, Throwable cause) {
        super(where == null ? kind.sentence() : kind.sentence() + ": " + where, cause);
        this.kind = kind;
    }

    /** A raise of a planned site of {@code kind} placed by {@code where} (phase D's Phase 2, raises as SITES,
     *  2026-11-16): its message the site's words, RENDERED AT PLANNING and read back from the plan -- the product's
     *  sentence, or legend-pure's in a plan the compatibility edge wrote -- never worded here. */
    public DataError(ErrorKind kind, @com.legend.base.Nullable String where, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    /** The product raise's kind, or null where the error is not one ({@link #DataError(String, Throwable)}). */
    public @com.legend.base.Nullable ErrorKind kind() {
        return kind;
    }

    /** An error decided at planning, raised at its turn from the test plan's data (docs/PLAN_AS_DATA_2026_11_07.md
     *  2.4, 2026-11-08): the message as it was planned, no cause (planning sends no statement). */
    public DataError(String message) {
        super(message);
        this.kind = null;
    }
}
