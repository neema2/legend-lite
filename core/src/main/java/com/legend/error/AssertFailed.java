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
public final class AssertFailed extends RuntimeException {

    private final @com.legend.base.Nullable String unjudgedReason;

    public AssertFailed(String message) {
        this(message, null);
    }

    private AssertFailed(String message, @com.legend.base.Nullable String unjudgedReason) {
        super(message);
        this.unjudgedReason = unjudgedReason;
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
}
