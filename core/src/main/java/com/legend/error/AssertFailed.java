// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * THE VERDICT-FAILURE CHANNEL (the exception seam, user directive
 * 2026-09-01): a statement-root assert adjudicated FALSE. Before the
 * seam this rode {@code java.sql.SQLException} — the runner's
 * historical failure contract — which made every verdict arm "speak
 * JDBC" and join the java.sql registers despite touching no database.
 * One meaning per type now: {@code AssertFailed} = the spec judged the
 * test's claim false; {@link DataError} = the database itself errored;
 * {@link NotImplementedException} = the feature is not built. Runtime
 * by design — a verdict terminates in the runner, never in a data
 * flow, so no intermediate layer should be forced to declare it.
 */
public final class AssertFailed extends RuntimeException implements LegendError {

    private final @com.legend.base.Nullable String unjudgedReason;
    /** The failure as data (item 3, 2026-11-21): its {@link AssertKind} and values; null for the failures that are not
     *  yet records -- the database judge's decline ({@link #unjudged}), its verdict row's sentence until item 3's S4
     *  carries the row's values, and the compatibility edge's appeal words (B3's). */
    private final @com.legend.base.Nullable ErrorRecord record;

    /** A failed assert as a record (item 3, 2026-11-21): its message is the record's sentence in lite's words. */
    public AssertFailed(ErrorRecord record) {
        super(record.sentence());
        if (!(record.code() instanceof AssertKind)) {
            throw new IllegalArgumentException("a failed assert's record is an assert's code: "
                    + record.code().qualified());
        }
        this.record = record;
        this.unjudgedReason = null;
    }

    /** A failure worded elsewhere, not yet a record: the database judge's verdict row until item 3's S4, and the
     *  compatibility edge's appeal path (B3's). */
    public AssertFailed(String message) {
        this(message, null);
    }

    private AssertFailed(String message, @com.legend.base.Nullable String unjudgedReason) {
        super(message);
        this.unjudgedReason = unjudgedReason;
        this.record = null;
    }

    /** The database judge DECLINED the assert's shape (leg 3.3): a failure
     * that carries WHY as a fact — the differential gate's ledger reads
     * it here, never from the message text. */
    public static AssertFailed unjudged(String reason, String message) {
        return new AssertFailed(message, reason);
    }

    /** Non-null iff this failure is an UNJUDGED decline. */
    public @com.legend.base.Nullable String unjudgedReason() {
        return unjudgedReason;
    }

    @Override
    public @com.legend.base.Nullable ErrorRecord record() {
        return record;
    }
}
