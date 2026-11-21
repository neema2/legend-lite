package com.legend.error;

/**
 * A name in the USER's source does not resolve — unknown or ambiguous
 * references, malformed imports ({@link Phase#RESOLVE}).
 */
public final class ResolutionException extends LegendCompileException {

    /** The error {@code record} states (stage B's BE, 2026-11-21). */
    public ResolutionException(ErrorRecord record) {
        super(Phase.RESOLVE, record, null);
    }

    public ResolutionException(String message) {
        super(Phase.RESOLVE, message);
    }
}
