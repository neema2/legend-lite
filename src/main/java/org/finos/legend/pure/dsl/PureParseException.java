package org.finos.legend.pure.dsl;

/**
 * Exception thrown when Pure language parsing fails.
 * Includes optional source location information for IDE integration.
 */
public class PureParseException extends RuntimeException {

    private final int line;
    private final int column;

    public PureParseException(String message) {
        super(message);
        this.line = -1;
        this.column = -1;
    }

    public PureParseException(String message, int line, int column) {
        super("line " + line + ":" + column + " " + message);
        this.line = line;
        this.column = column;
    }

    public PureParseException(String message, Throwable cause) {
        super(message, cause);
        this.line = -1;
        this.column = -1;
    }

    public int getLine() {
        return line;
    }

    public int getColumn() {
        return column;
    }

    public boolean hasLocation() {
        return line >= 0 && column >= 0;
    }
}
