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
public final class DataError extends RuntimeException implements LegendError {

    /** The product raise's kind (phase D's D1.1), or null: a database's own error, the edge's sentence, a user's
     *  {@code fail} message. */
    private final @com.legend.base.Nullable ErrorKind kind;

    /** The SQLSTATE the driver gave a database's own failure, or null. */
    private final @com.legend.base.Nullable String sqlState;

    /** The database's own message, verbatim, where the error is the database's ({@link ErrorKind#isDatabases}). */
    private final @com.legend.base.Nullable String databaseMessage;

    /** Where the raise happened, as its constructor was given it (a site's where: a parameter's name, the digits a
     *  database keeps), or null. Kept since PR #118's fix round (2026-11-18, N3): until then it was taken and dropped,
     *  so lite's API could not answer it. */
    private final @com.legend.base.Nullable String where;
    /** The error as data where its kind and place are its facts (stage B's BE, 2026-11-21): null for a database's own
     *  failure and an error of no kind, whose words are their own. */
    private final @com.legend.base.Nullable ErrorRecord record;

    public DataError(String message, Throwable cause) {
        super(message, cause);
        this.kind = null;
        this.sqlState = null;
        this.databaseMessage = null;
        this.where = null;
        this.record = null;
    }

    // (the constructors of a kind and its where -- (kind, cause), (kind, where, cause) and (kind, where, message,
    // cause) -- gone 2026-11-21, the BE audit 2's S10: no product path made one, and a kind's sentence reads its
    // operands, which they put in its first slot; a raise is its record, DataError(record, where, cause))

    private static ErrorRecord fileTooLargeRecord(String path, long bytes, long characters, int limitMegabytes) {
        return ErrorRecord.of(ErrorKind.FILE_TOO_LARGE, java.util.List.of(path, Long.toString(bytes),
                Long.toString(characters), Integer.toString(limitMegabytes)));
    }

    /** The error {@code record} states, its code a run's {@link ErrorKind}, with no place: its message
     *  the record's sentence (stage B's BE, 2026-11-21; a planned failure rebuilt from its record). */
    public DataError(ErrorRecord record) {
        // no place: a planned failure carries none (the BE audit 2's nit 4: its first value was taken for it)
        this(record, null, null);
    }

    /** A raise of a planned site (stage B's BE, 2026-11-21, the one catalog): its record the site's kind and its
     *  operands as the kind's sentence prints them ({@code result.RaiseWords}), its message the record's sentence --
     *  the one catalog's, upstream's words -- and {@code where} the site's place, a fact lite's API answers (PR #118's
     *  fix round, N3), never in the sentence. */
    public DataError(ErrorRecord record, @com.legend.base.Nullable String where,
            @com.legend.base.Nullable Throwable cause) {
        super(record.sentence(), cause);
        if (!(record.code() instanceof ErrorKind k)) {
            throw new IllegalArgumentException("a data error's record is a run's error: " + record.code());
        }
        this.kind = k;
        this.sqlState = null;
        this.databaseMessage = null;
        this.where = where;
        this.record = record;
    }

    /** A load's file larger than the load admits ({@link ErrorKind#FILE_TOO_LARGE}): its record the file, its size in
     *  bytes and in characters, and the limit in megabytes, so an audience words the size as it does (stage B's BE,
     *  2026-11-21: until then its facts rode {@code base.DataSize.TooLarge}, its cause, alone). */
    public static DataError fileTooLarge(String path, long bytes, long characters, int limitMegabytes, Throwable cause) {
        return new DataError(fileTooLargeRecord(path, bytes, characters, limitMegabytes), path, cause);
    }

    @Override
    public @com.legend.base.Nullable ErrorRecord record() {
        return record;
    }

    private DataError(ErrorKind kind, @com.legend.base.Nullable String sqlState, String databaseMessage, String message,
            Throwable cause) {
        super(message, cause);
        if (!kind.isDatabases()) {
            throw new IllegalArgumentException("a database's own failure is DATABASE_ERROR or CONNECTION_ERROR, not "
                    + kind);
        }
        this.kind = kind;
        this.sqlState = sqlState;
        this.databaseMessage = databaseMessage;
        this.where = null;
        this.record = null;
    }

    /** A failure the database raised itself (phase D's Phase 2, item 4, 2026-11-16): {@code kind} DATABASE_ERROR or
     *  CONNECTION_ERROR, its SQLSTATE where the driver gave one, its message the database's own, verbatim
     *  ({@code message}: that text, or that text named by what the statement was, as a load's refusal names its data). */
    public static DataError database(ErrorKind kind, @com.legend.base.Nullable String sqlState, String databaseMessage,
            String message, Throwable cause) {
        return new DataError(kind, sqlState, databaseMessage, message, cause);
    }

    /** The SQLSTATE of a database's own failure, where the driver gave one; null otherwise. */
    public @com.legend.base.Nullable String sqlState() {
        return sqlState;
    }

    /** The database's own message, verbatim, where the error is the database's ({@link ErrorKind#isDatabases}); null
     *  for a raise, a user's fail or lite's own refusal. */
    public @com.legend.base.Nullable String databaseMessage() {
        return databaseMessage;
    }

    /** Where the raise happened, as its constructor was given it, or null. */
    public @com.legend.base.Nullable String where() {
        return where;
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
        this.sqlState = null;
        this.databaseMessage = null;
        this.where = null;
        this.record = null;
    }
}
