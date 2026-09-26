package com.legend.protocol;

import com.legend.protocol.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;

/**
 * A derived (computed) property declaration — a parse product. Body is parsed eagerly
 * by {@code ElementParser} into a sequence of {@link ValueSpecification} statements
 * (the body grammar matches a function body's braced block).
 *
 * <p>Formerly nested in {@code com.legend.model.ClassDefinition}; lifted to the protocol
 * layer because the parser's output types must not depend on the model.
 *
 * @param name        property name
 * @param parameters  parameter list (zero or more)
 * @param realization inline body or function-ref binding
 * @param type        return type
 * @param multiplicity declared multiplicity
 */
public record DerivedPropertyDefinition(
        String name,
        List<ParameterDefinition> parameters,
        Realization realization,
        TypeExpression type,
        Multiplicity multiplicity,
        List<Protocol.PStereotype> stereotypes,
        List<Protocol.PTaggedValue> taggedValues,
        @com.legend.base.Nullable SourceInfo pos) {
    public DerivedPropertyDefinition {
        Objects.requireNonNull(name, "Derived property name cannot be null");
        Objects.requireNonNull(type, "Derived property type cannot be null");
        Objects.requireNonNull(multiplicity, "Derived property multiplicity cannot be null");
        Objects.requireNonNull(realization, "Derived property realization cannot be null");
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }

    /** Position-free form for synthesis and tests. */
    public DerivedPropertyDefinition(String name, List<ParameterDefinition> parameters,
                                     Realization realization,
                                     TypeExpression type, Multiplicity multiplicity) {
        this(name, parameters, realization, type, multiplicity, List.of(), List.of(), null);
    }

    /** Convenience: the sugar (inline-expression) form. */
    public DerivedPropertyDefinition(String name, List<ParameterDefinition> parameters,
                                     List<ValueSpecification> expression,
                                     TypeExpression type, Multiplicity multiplicity) {
        this(name, parameters, new Realization.Inline(expression), type, multiplicity,
                List.of(), List.of(), null);
    }

    /** Position is excluded from equality — same contract as the value-spec records. */
    @Override
    public boolean equals(Object o) {
        return o instanceof DerivedPropertyDefinition other
                && name.equals(other.name())
                && parameters.equals(other.parameters())
                && realization.equals(other.realization())
                && type.equals(other.type())
                && multiplicity.equals(other.multiplicity())
                && stereotypes.equals(other.stereotypes())
                && taggedValues.equals(other.taggedValues());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, parameters, realization, type, multiplicity,
                stereotypes, taggedValues);
    }

    /**
     * The inline body (sugar form). Valid only when the realization is an
     * {@link Realization.Inline}; a Door-4 function-ref binding has no inline
     * body (its realizing function is the bound FQN).
     */
    public List<ValueSpecification> expression() {
        if (realization instanceof Realization.Inline inl) return inl.body();
        throw new IllegalStateException(
                "derived property '" + name + "' is a function-ref binding, not an inline body");
    }
}
