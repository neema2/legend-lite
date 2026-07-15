package com.legend.model.spec;

/**
 * Float literal. Example Pure source: {@code 3.14} or {@code 1.5e-3}.
 *
 * <p>Stored as {@code double} to mirror the engine record. Precision-
 * sensitive consumers should use {@link CDecimal} (Pure suffix
 * {@code d}, e.g. {@code 3.14d}). C.1 does not yet emit
 * {@link CDecimal}; that variant lands in C.5.
 */
public record CFloat(double value) implements ValueSpecification {
}
