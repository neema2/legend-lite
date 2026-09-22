// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * THE PROVENANCE SEAM for platform-raised error messages (truthfulness
 * burn B7, ADAPTER_NECESSITY_CENSUS §5c).
 *
 * <p>The lowering raises pure's own error messages IN SQL (the
 * {@code SqlFn.ERROR} guards — domain checks, bounds, bit-shift
 * limits), and every driver wraps a raised message in its own
 * transport envelope (DuckDB prefixes {@code Invalid Input Error: };
 * H2 appends statement context to SIGNAL text). The message inside is
 * OURS — composed by the compiler, pure's exact text — and the
 * envelope is the carrier's.
 *
 * <p>The renderer marks every raised message with a U+001F sentinel at
 * BOTH ends; this seam extracts BETWEEN the sentinels, which removes
 * the envelope wherever the driver put it (prefix, suffix, both). A
 * message WITHOUT the sentinel pair is a NATIVE database error and
 * passes through untouched — its error class and envelope survive, so
 * an envelope-blind strip can never launder a native error into pure's
 * expected text (the deep-audit H4 class-erasure weakness dies here).
 * ONE owner: the old per-consumer strips (the PCT adapter's
 * remapErrorMessage, AssertErrorNative's broad {@code "* Error: "}
 * regex) are deleted in the same batch.
 */
public final class RaisedErrors {

    /** U+001F (unit separator) — the provenance mark the renderer puts
     * around raised messages. Never appears in pure's own error text. */
    public static final char SENTINEL = '';

    /** U+001E (record separator) — divides the OPTIONAL source-position
     * prefix ({@code line:col}, the raising call's name-token span) from
     * the message INSIDE the envelope (leg 2 — interpreted AssertError
     * hands the matcher the raising expression's source info). Only ever
     * read between a sentinel pair, so a native error can't fake it. */
    public static final char POSITION_SEP = '';

    private RaisedErrors() {
    }

    /** The message with the transport envelope removed IF this is a
     * platform-raised message (sentinel pair present); unchanged
     * otherwise. The in-envelope position prefix strips too —
     * production text carries NO wire protocol. */
    public static @com.legend.base.Nullable String unwrap(
            @com.legend.base.Nullable String message) {
        if (message == null) {
            return null;
        }
        int first = message.indexOf(SENTINEL);
        // the envelope CLOSES at the next mark, never the last one in the
        // message: a driver that embeds the statement text (H2) carries the
        // raise's own literal a second time further on
        int last = first < 0 ? -1 : message.indexOf(SENTINEL, first + 1);
        if (first < 0 || last <= first) {
            return message;
        }
        String inner = message.substring(first + 1, last);
        int sep = inner.indexOf(POSITION_SEP);
        return sep >= 0 ? inner.substring(sep + 1) : inner;
    }

    /** A raised error whose emission carried its pure source position
     * (the raising call's name-token span — {@code PureSql.raise}) —
     * assertError's position channel. Message is the CLEAN text. */
    public static final class Positioned extends java.sql.SQLException {
        private final int line;
        private final int column;

        Positioned(String message, @com.legend.base.Nullable String sqlState,
                java.sql.SQLException cause, int line, int column) {
            super(message, sqlState, cause);
            this.line = line;
            this.column = column;
        }

        public int line() {
            return line;
        }

        public int column() {
            return column;
        }
    }

    /** Funnel rethrow: a raised-message SQLException re-surfaces with
     * the clean text (state and cause preserved) — as {@link Positioned}
     * when the emission carried its source span; everything else
     * returns as-is. */
    public static java.sql.SQLException unwrapped(java.sql.SQLException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return e;
        }
        int first = msg.indexOf(SENTINEL);
        int last = first < 0 ? -1 : msg.indexOf(SENTINEL, first + 1);   // see unwrap
        if (first < 0 || last <= first) {
            return e;
        }
        String inner = msg.substring(first + 1, last);
        int sep = inner.indexOf(POSITION_SEP);
        if (sep < 0) {
            return new java.sql.SQLException(inner, e.getSQLState(), e);
        }
        String pos = inner.substring(0, sep);
        String clean = inner.substring(sep + 1);
        int colon = pos.indexOf(':');
        if (colon > 0) {
            try {
                return new Positioned(clean, e.getSQLState(), e,
                        Integer.parseInt(pos.substring(0, colon)),
                        Integer.parseInt(pos.substring(colon + 1)));
            } catch (NumberFormatException ignored) {
                // a malformed prefix falls through to the plain strip
            }
        }
        return new java.sql.SQLException(clean, e.getSQLState(), e);
    }
}
