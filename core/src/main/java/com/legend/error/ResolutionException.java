package com.legend.error;

/**
 * A name in the USER's source does not resolve — unknown or ambiguous
 * references, malformed imports ({@link Phase#RESOLVE}).
 */
public final class ResolutionException extends LegendCompileException {

    public ResolutionException(String message) {
        super(Phase.RESOLVE, message);
    }
}
