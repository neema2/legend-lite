package com.legend.error;

/**
 * The input may be valid; the FEATURE is not built. One greppable type for
 * the whole backlog (was: a mix of UnsupportedOperationException and
 * IllegalStateException "not yet implemented" — AUDIT_2026_07 §11).
 */
public final class NotImplementedException extends RuntimeException {

    public NotImplementedException(String message) {
        super(message);
    }
}
