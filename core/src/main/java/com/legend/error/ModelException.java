package com.legend.error;

/**
 * A semantic error in the USER's model — bad mapping shapes, unknown
 * tables/filters/classes referenced by elements, malformed hierarchies.
 * Raised by the normalizer ({@link Phase#NORMALIZE}) and element
 * compilation ({@link Phase#MODEL}).
 */
public final class ModelException extends LegendCompileException {

    public ModelException(Phase phase, String message) {
        super(phase, message);
    }
}
