package org.finos.legend.pure.dsl;

/**
 * Exception thrown when Pure language parsing fails.
 */
public class PureParseException extends RuntimeException {
    
    public PureParseException(String message) {
        super(message);
    }
    
    public PureParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
