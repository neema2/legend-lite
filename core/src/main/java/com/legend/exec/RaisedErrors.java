// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import java.util.List;

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
     * The site a raise's text names (phase D's Phase 2, raises as SITES, 2026-11-16): a text that is a site's token
     * ({@code LG#<n>}, {@link com.legend.sql.RaiseKind#token}) is site {@code n} of the statement that raised it, read
     * from the sites its plan declares for it; null for any other text -- a user's {@code fail} message. A token the
     * statement does not declare is refused by name: the database
     * reported what the plan did not plan.
     */
    static ExecutionPlan.@com.legend.base.Nullable Site siteOf(String text, List<ExecutionPlan.Site> sites) {
        int n = com.legend.sql.RaiseKind.siteNumber(text);
        if (n < 0) {
            return null;
        }
        if (n > sites.size()) {
            throw new IllegalStateException("the database raised " + text + ", which the statement does not declare"
                    + " (it declares " + sites.stream().map(s -> com.legend.sql.RaiseKind.token(s.n()) + " "
                    + s.kind()).toList() + ")");
        }
        return sites.get(n - 1);
    }

    /** A failure that IS a raise (pure's own error, its text the platform's: the sentinel envelope unwrapped), told by
     *  its type wherever a caller asks whether a failure was raised (the B checkpoint's tenets audit, LOW 4,
     *  2026-11-11: until then the seam compared a failure's message with its database's to tell). A raise of a site
     *  carries the site's record (its kind, where and operands; no words: stage B's B1, 2026-11-20); its message the
     *  kind behind its mark and the where behind a second, lite's carriage, never words. */
    public static class Raised extends java.sql.SQLException implements ExecutionPlan.RaisedAtSite {
        private final ExecutionPlan.@com.legend.base.Nullable Site site;

        // (its statement's text, carried beside its site for the edge's recorded words (K10b, 2026-11-17), went with
        // the recorder: stage B's B1, 2026-11-20, the edge words a site from its record)
        Raised(String text, ExecutionPlan.@com.legend.base.Nullable Site site, @com.legend.base.Nullable String sqlState,
                java.sql.SQLException cause) {
            super(site == null ? text : com.legend.sql.RaiseKind.MARK + site.kind().name(), sqlState, cause);
            this.site = site;
        }

        /** The kind of the site the raise named ({@link #siteOf}); null for the edge's sentence or a user's message. */
        public com.legend.sql.@com.legend.base.Nullable RaiseKind kind() {
            return site == null ? null : site.kind();
        }

        /** The site the raise named, null where its text named none. */
        @Override
        public ExecutionPlan.@com.legend.base.Nullable Site site() {
            return site;
        }
    }

    /** A raised error whose emission carried its pure source position
     * (the raising call's name-token span — {@code PureSql.raise}) —
     * assertError's position channel. Message is the CLEAN text. */
    public static final class Positioned extends Raised {
        private final int line;
        private final int column;

        Positioned(String message, ExecutionPlan.@com.legend.base.Nullable Site site,
                @com.legend.base.Nullable String sqlState, java.sql.SQLException cause, int line, int column) {
            super(message, site, sqlState, cause);
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

    /** {@link #unwrapped(java.sql.SQLException, List)}, or, for a failure whose text carries no raise, what {@code held}
     *  reads back then -- the raise the session it failed on held ({@link JdbcStatements#held}: a database whose errors
     *  cannot carry a raise's text keeps it in the session), unwrapped; the error as it is where that is null. The
     *  session is asked only then: a failure whose text carries its raise costs no read. */
    public static java.sql.SQLException unwrapped(java.sql.SQLException e, List<ExecutionPlan.Site> sites,
            java.util.function.Supplier<@com.legend.base.Nullable String> held) {
        java.sql.SQLException un = unwrapped(e, sites);
        if (un != e) {
            return un;
        }
        String text = held.get();
        return text == null ? e : unwrapped(new java.sql.SQLException(text, e.getSQLState(), e), sites);
    }

    /** Funnel rethrow: a raised-message SQLException re-surfaces with the clean text (state and cause preserved), a
     *  site's token read through {@code sites}, the sites the plan declares for the statement that raised it -- as
     *  {@link Positioned} when the emission carried its source span; everything else returns as-is. */
    public static java.sql.SQLException unwrapped(java.sql.SQLException e, List<ExecutionPlan.Site> sites) {
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
            return new Raised(inner, siteOf(inner, sites), e.getSQLState(), e);
        }
        String pos = inner.substring(0, sep);
        String clean = inner.substring(sep + 1);
        ExecutionPlan.Site site = siteOf(clean, sites);
        int colon = pos.indexOf(':');
        if (colon > 0) {
            try {
                return new Positioned(clean, site, e.getSQLState(), e,
                        Integer.parseInt(pos.substring(0, colon)),
                        Integer.parseInt(pos.substring(colon + 1)));
            } catch (NumberFormatException ignored) {
                // a malformed prefix falls through to the plain strip
            }
        }
        return new Raised(clean, site, e.getSQLState(), e);
    }

    /** Where in the query {@code raised} happened, the {@code [line:col]} its envelope carried, or null. */
    private static @com.legend.base.Nullable String positionOf(java.sql.SQLException raised) {
        return raised instanceof Positioned p ? "[" + p.line() + ":" + p.column() + "]" : null;
    }

    /** A failed open's or close's error (no statement, so no site it can name) as the
     *  {@link com.legend.error.DataError} the platform tells: its raise unwrapped. Here since D70 (2026-10-30; the plan
     *  sessions' until then): the one class that constructs from a {@code SQLException}. */
    static com.legend.error.DataError dataError(java.sql.SQLException e) {
        return dataErrorOf(unwrapped(e, List.of()));
    }

    /** A failure whose text is {@code message} (a driver that reports no {@code SQLException}: MongoDB's) as the
     *  platform's typed error: its raise unwrapped, a site's token read through {@code sites} (the statement's), else
     *  the text (or the message). */
    public static com.legend.error.DataError dataError(String message, Throwable cause, List<ExecutionPlan.Site> sites) {
        String text = unwrap(message);
        ExecutionPlan.Site site = text == null ? null : siteOf(text, sites);
        if (text != null && site != null) {
            // unwrapped as a statement's failure is, so the envelope's position (its [line:col]) is read too (the BE
            // audit 4's M3, 2026-11-21: a Raised built from the stripped text had none, so MongoDB's had no where)
            return dataErrorOf(unwrapped(new java.sql.SQLException(message, cause), sites));
        }
        if (text != null && !text.equals(message)) {
            // lite's envelope, no site: a user's fail
            return new com.legend.error.DataError(text, cause);
        }
        // no envelope: the database's own failure, its message verbatim (a document store's: no SQLSTATE)
        String own = String.valueOf(message);
        return com.legend.error.DataError.database(com.legend.error.ErrorKind.DATABASE_ERROR, null, own, own, cause);
    }

    /** {@code un} as the platform's typed error: a site's raise by its kind and its planned words (phase D's Phase 2;
     *  K10a, 2026-11-17: the product's sentence alone, the site's message -- never the plan's upstream words), any other failure by its text. */
    static com.legend.error.DataError dataErrorOf(java.sql.SQLException un) {
        if (un instanceof Raised r && r.site() != null) {
            ExecutionPlan.Site site = r.site();
            return new com.legend.error.DataError(com.legend.result.RaiseWords.record(site), positionOf(un), un);
        }
        if (un instanceof Raised) {
            // lite's envelope, no site: a user's fail (its own text) --
            // never the database's (phase D's Phase 2, item 4)
            return new com.legend.error.DataError(String.valueOf(un.getMessage()), un);
        }
        // no envelope: the database's own failure (phase D's Phase 2, item 4, 2026-11-16), its message verbatim and its
        // SQLSTATE; a connection lost (SQLSTATE class 08) a connection's failure
        String own = String.valueOf(un.getMessage());
        return com.legend.error.DataError.database(lost(un) ? com.legend.error.ErrorKind.CONNECTION_ERROR
                : com.legend.error.ErrorKind.DATABASE_ERROR, un.getSQLState(), own, own, un);
    }

    /** Whether {@code e} is a connection's failure: JDBC's own types of the SQLSTATE class 08 (connection exception),
     *  which the driver chooses -- exec reads the type, never the text. */
    private static boolean lost(java.sql.SQLException e) {
        return e instanceof java.sql.SQLNonTransientConnectionException
                || e instanceof java.sql.SQLTransientConnectionException;
    }

    /** A session's open or close that failed (phase D's Phase 2, item 4, 2026-11-16): {@code CONNECTION_ERROR}, the
     *  database's own message verbatim and its SQLSTATE; no statement ran, so no raise can be in it. */
    static com.legend.error.DataError connectionError(java.sql.SQLException e) {
        String own = String.valueOf(e.getMessage());
        return com.legend.error.DataError.database(com.legend.error.ErrorKind.CONNECTION_ERROR, e.getSQLState(), own, own,
                e);
    }

    /** A failed statement's error, its sites the plan's for it, with the raise the session holds read back where the
     *  error carries none ({@link #unwrapped(java.sql.SQLException, List, java.util.function.Supplier)}). */
    static com.legend.error.DataError dataError(java.sql.SQLException e, List<ExecutionPlan.Site> sites,
            java.util.function.Supplier<@com.legend.base.Nullable String> held) {
        return dataErrorOf(unwrapped(e, sites, held));
    }

}
