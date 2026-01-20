package org.finos.legend.engine.sql;

/**
 * Exception thrown when SQL compilation fails.
 */
public class SQLCompileException extends RuntimeException {
    
    public SQLCompileException(String message) {
        super(message);
    }
    
    public SQLCompileException(String message, Throwable cause) {
        super(message, cause);
    }
}
