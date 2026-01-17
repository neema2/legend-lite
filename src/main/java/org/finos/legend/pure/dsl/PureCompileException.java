package org.finos.legend.pure.dsl;

/**
 * Exception thrown when Pure language compilation fails.
 */
public class PureCompileException extends RuntimeException {
    
    public PureCompileException(String message) {
        super(message);
    }
    
    public PureCompileException(String message, Throwable cause) {
        super(message, cause);
    }
}
