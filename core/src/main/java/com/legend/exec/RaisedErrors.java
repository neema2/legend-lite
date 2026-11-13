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

    /** U+001D (group separator) -- a QUOTE inside the envelope, for a carrier that would re-quote one: SQLite raises a
     *  message as a bad JSON path, and its error text quotes the path SQL-style ({@code '} doubled, measured
     *  2026-10-11), so its dialect writes each quote of the message as this mark. Only ever read between a sentinel
     *  pair. */
    public static final char QUOTE_MARK = '';

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
        String inner = message.substring(first + 1, last).replace(QUOTE_MARK, '\'');
        int sep = inner.indexOf(POSITION_SEP);
        return sep >= 0 ? inner.substring(sep + 1) : inner;
    }

    /**
     * The kind a product raise names (phase D's D1.1): its text IS a {@link com.legend.sql.RaiseKind}'s name, the
     * product's dialects writing nothing else ({@code AnsiSqlRenderer.raiseMessage}); null for any other text -- the
     * edge's sentence, a user's {@code fail} message. A user's message spelled exactly as a kind's name reads as that
     * kind (a named limit: the envelope carries the text alone).
     */
    static com.legend.sql.@com.legend.base.Nullable RaiseKind kindOf(String text) {
        for (com.legend.sql.RaiseKind k : com.legend.sql.RaiseKind.values()) {
            if (k.name().equals(text)) {
                return k;
            }
        }
        return null;
    }


    /** A failure that IS a raise (pure's own error, its text the platform's: the sentinel envelope unwrapped), told by
     *  its type wherever a caller asks whether a failure was raised (the B checkpoint's tenets audit, LOW 4,
     *  2026-11-11: until then the seam compared a failure's message with its database's to tell). */
    public static class Raised extends java.sql.SQLException {
        private final com.legend.sql.@com.legend.base.Nullable RaiseKind kind;

        Raised(String message, @com.legend.base.Nullable String sqlState, java.sql.SQLException cause) {
            super(message, sqlState, cause);
            this.kind = kindOf(message);
        }

        /** The kind a product raise names ({@link #kindOf}); null for the edge's sentence or a user's message. */
        public com.legend.sql.@com.legend.base.Nullable RaiseKind kind() {
            return kind;
        }
    }

    /** A raised error whose emission carried its pure source position
     * (the raising call's name-token span — {@code PureSql.raise}) —
     * assertError's position channel. Message is the CLEAN text. */
    public static final class Positioned extends Raised {
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

    /** {@link #unwrapped(java.sql.SQLException)}, or, for a failure whose text carries no raise, what {@code held}
     *  reads back then -- the raise the session it failed on held ({@link JdbcStatements#held}: a database whose errors
     *  cannot carry a raise's text keeps it in the session), unwrapped; the error as it is where that is null. The
     *  session is asked only then: a failure whose text carries its raise costs no read. */
    public static java.sql.SQLException unwrapped(java.sql.SQLException e,
            java.util.function.Supplier<@com.legend.base.Nullable String> held) {
        java.sql.SQLException un = unwrapped(e);
        if (un != e) {
            return un;
        }
        String text = held.get();
        return text == null ? e : unwrapped(new java.sql.SQLException(text, e.getSQLState(), e));
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
        String inner = msg.substring(first + 1, last).replace(QUOTE_MARK, '\'');
        int sep = inner.indexOf(POSITION_SEP);
        if (sep < 0) {
            return new Raised(inner, e.getSQLState(), e);
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
        return new Raised(clean, e.getSQLState(), e);
    }

    /** A failed statement's or open's error as the {@link com.legend.error.DataError} the platform tells: its raise
     *  unwrapped ({@link #unwrapped(java.sql.SQLException)}). Here since D70 (2026-10-30; the plan sessions' until
     *  then): the one class that constructs from a {@code SQLException}. */
    static com.legend.error.DataError dataError(java.sql.SQLException e) {
        return dataErrorOf(unwrapped(e));
    }

    /** A failure whose text is {@code message} (a driver that reports no {@code SQLException}: MongoDB's) as the
     *  platform's typed error: its raise unwrapped, a product raise by its kind, else the text (or the message). */
    public static com.legend.error.DataError dataError(String message, Throwable cause) {
        String text = unwrap(message);
        com.legend.sql.RaiseKind kind = text == null ? null : kindOf(text);
        return kind != null ? new com.legend.error.DataError(com.legend.error.ErrorKind.valueOf(kind.name()), cause)
                : new com.legend.error.DataError(text == null ? String.valueOf(message) : text, cause);
    }

    /** {@code un} as the platform's typed error: a product raise by its kind (the error layer spells its sentence,
     *  {@code error.ErrorKind}; phase D's D1.1), any other failure by its text. */
    static com.legend.error.DataError dataErrorOf(java.sql.SQLException un) {
        if (un instanceof Raised r && r.kind() != null) {
            return new com.legend.error.DataError(com.legend.error.ErrorKind.valueOf(r.kind().name()), un);
        }
        return new com.legend.error.DataError(String.valueOf(un.getMessage()), un);
    }

    /** {@link #dataError(java.sql.SQLException)} with the raise the session holds read back where the error carries
     *  none ({@link #unwrapped(java.sql.SQLException, java.util.function.Supplier)}). */
    static com.legend.error.DataError dataError(java.sql.SQLException e,
            java.util.function.Supplier<@com.legend.base.Nullable String> held) {
        return dataErrorOf(unwrapped(e, held));
    }
}
