package com.legend.compiler.spec;

/**
 * Phase-G type-inference failure &mdash; a body (or a native-call argument) does
 * not type-check: unification conflict, an unbound type/multiplicity variable
 * at resolution, or an unsatisfied subtype constraint.
 */
public class TypeInferenceException extends RuntimeException {
    public TypeInferenceException(String message) {
        super(message);
    }

    public TypeInferenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
