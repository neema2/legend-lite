package com.legend.protocol;

import java.util.Objects;

/**
 * A parameter declaration on a derived property or function — a parse product.
 *
 * <p>Formerly nested in {@code com.legend.model.ClassDefinition}; lifted to the protocol
 * layer because it is pure syntax (name, type expression, declared multiplicity) and the
 * parser's output types must not depend on the model.
 *
 * @param name         parameter name
 * @param type         parameter type
 * @param multiplicity declared multiplicity (concrete or parameter ref)
 */
public record ParameterDefinition(
        String name,
        TypeExpression type,
        Multiplicity multiplicity,
        @com.legend.base.Nullable SourceInfo pos) {
    public ParameterDefinition {
        Objects.requireNonNull(name, "Parameter name cannot be null");
        Objects.requireNonNull(type, "Parameter type cannot be null");
        Objects.requireNonNull(multiplicity, "Parameter multiplicity cannot be null");
    }

    /** Position-free form for synthesis and tests. The parser sets the span of the whole
     *  `name: Type[mult]` declaration (engine convention). */
    public ParameterDefinition(String name, TypeExpression type, Multiplicity multiplicity) {
        this(name, type, multiplicity, null);
    }

    /** Position is excluded from equality — same contract as the value-spec records. */
    @Override
    public boolean equals(Object o) {
        return o instanceof ParameterDefinition other
                && name.equals(other.name())
                && type.equals(other.type())
                && multiplicity.equals(other.multiplicity());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, multiplicity);
    }
}
