// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

/**
 * A Postgres query with no Pure translation, by {@link Kind}: text that is not SQL, SQL whose translation is not
 * built yet, or a query Postgres itself rejects ("column x must appear in the GROUP BY clause", with Postgres's
 * wording). It carries the 1-based line and column in the SQL text, so the caller places it in its own source.
 */
public final class SqlTranslationException extends RuntimeException {

    /** Why there is no translation. */
    public enum Kind {
        /** The text is not Postgres SQL. */
        SYNTAX,
        /** Postgres SQL whose translation is not built yet: the input may be fine. */
        UNSUPPORTED,
        /** A query Postgres rejects too: an unknown column, an ambiguous name, a misplaced aggregate. */
        INVALID
    }

    private final Kind kind;
    private final int line;
    private final int column;
    private final boolean pastSizeBound;

    public SqlTranslationException(Kind kind, String message, int line, int column) {
        this(kind, message, line, column, false);
    }

    private SqlTranslationException(Kind kind, String message, int line, int column, boolean pastSizeBound) {
        super(message);
        this.kind = kind;
        this.line = line;
        this.column = column;
        this.pastSizeBound = pastSizeBound;
    }

    /** IQ island, 2026-10-17: a translation refused for its size alone (a dialect's bound on the Pure it writes), which
     *  the translator answers by computing the item's long operands once each and trying again. */
    public static SqlTranslationException pastSizeBound(String message, SqlAst.Pos pos) {
        return new SqlTranslationException(Kind.UNSUPPORTED, message, pos.line(), pos.column(), true);
    }

    /** Whether this is a refusal for size alone ({@link #pastSizeBound}). */
    public boolean pastSizeBound() {
        return pastSizeBound;
    }

    public static SqlTranslationException unsupported(String message, SqlAst.Pos pos) {
        return new SqlTranslationException(Kind.UNSUPPORTED, message, pos.line(), pos.column());
    }

    public static SqlTranslationException invalid(String message, SqlAst.Pos pos) {
        return new SqlTranslationException(Kind.INVALID, message, pos.line(), pos.column());
    }

    public Kind kind() {
        return kind;
    }

    public int line() {
        return line;
    }

    public int column() {
        return column;
    }
}
