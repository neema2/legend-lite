package com.legend.compiler.element.type;


import java.util.Objects;

/**
 * An expression type: a {@link Type} paired with its {@link Multiplicity}
 * (engine {@code ExpressionType}). It is the carrier on every typed node's
 * {@code info()} (the HIR), and the form of a native call's argument and result
 * types in the {@link InferenceKernel}.
 */
public record ExprType(Type type, Multiplicity multiplicity) {
    public ExprType {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(multiplicity, "multiplicity");
    }

    /** The common case: {@code type[1]}. */
    public static ExprType one(Type type) {
        return new ExprType(type, Multiplicity.Bounded.ONE);
    }
}
