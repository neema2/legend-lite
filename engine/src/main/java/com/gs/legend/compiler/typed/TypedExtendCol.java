package com.gs.legend.compiler.typed;

/**
 * Extend column for {@link TypedExtend}. Covers scalar extends, window extends,
 * and traverse extends via sealed subvariants.
 */
public sealed interface TypedExtendCol permits
        TypedScalarExtendCol, TypedWindowExtendCol, TypedTraverseExtendCol,
        TypedAssociationExtendCol, TypedEmbeddedExtendCol {
    String alias();
}
