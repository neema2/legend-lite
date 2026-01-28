package org.finos.legend.engine.sql;

/**
 * Exception thrown when SQL parsing fails.
 */
public class SQLParseException extends RuntimeException {

    private final int position;

    public SQLParseException(String message, int position) {
        super(message + " at position " + position);
        this.position = position;
    }

    public int getPosition() {
        return position;
    }
}
