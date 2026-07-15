package com.legend.model;

import java.util.Objects;

/**
 * {@code Primitive my::pkg::ExtendedString extends String} — a PRECISE
 * PRIMITIVE (real m3 primitive extension). For query compilation the
 * extension is its base primitive: values, equality, and casts all carry
 * base-primitive semantics (real pure's precise-primitive CONSTRAINTS are
 * instantiation-time validations, out of the query path). The definition
 * registers so {@code cast(@ExtendedString)} and property declarations
 * resolve; the constraint block, when present, is parsed and dropped.
 */
public record PrimitiveExtensionDefinition(
        String qualifiedName,
        String baseTypeName) implements PackageableElement {

    public PrimitiveExtensionDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(baseTypeName, "Base type cannot be null");
    }
}
