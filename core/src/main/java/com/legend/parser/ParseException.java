package com.legend.parser;

/**
 * Thrown by {@link ElementParser} and related parsers when the token
 * stream doesn't match the expected Pure grammar.
 *
 * <p>Carries source line/column when known; both default to {@code 0}
 * for errors that occur outside any token (e.g. unexpected EOF on an
 * empty stream). Unchecked &mdash; parser callers don't typically catch
 * these except at the top of a pipeline.
 */
public final class ParseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int line;
    private final int column;

    public ParseException(String message) {
        super(message);
        this.line = 0;
        this.column = 0;
    }

    public ParseException(String message, int line, int column) {
        super(formatMessage(message, line, column));
        this.line = line;
        this.column = column;
    }

    public int line() {
        return line;
    }

    public int column() {
        return column;
    }

    private static String formatMessage(String message, int line, int column) {
        if (line <= 0) return message;
        return "[" + line + ":" + column + "] " + message;
    }
}
