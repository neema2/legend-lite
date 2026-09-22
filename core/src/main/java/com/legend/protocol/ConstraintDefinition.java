package com.legend.protocol;

import com.legend.protocol.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;

/**
 * A class-level constraint (validation rule) — a parse product. The constraint body is
 * a single Pure expression that must evaluate to a {@code Boolean};
 * {@code ElementParser} parses it eagerly into a {@link ValueSpecification}.
 *
 * <p>Formerly nested in {@code com.legend.model.ClassDefinition}; lifted to the protocol
 * layer because the parser's output types must not depend on the model.
 *
 * @param name       constraint name
 * @param realization inline predicate or function-ref binding
 */
public record ConstraintDefinition(String name, Realization realization,
        @com.legend.base.Nullable ValueSpecification message,
        @com.legend.base.Nullable String enforcementLevel,
        @com.legend.base.Nullable String externalId,
        @com.legend.base.Nullable String owner,
        @com.legend.base.Nullable SourceInfo pos) {
    public ConstraintDefinition {
        Objects.requireNonNull(name, "Constraint name cannot be null");
        Objects.requireNonNull(realization, "Constraint realization cannot be null");
    }

    /** Position-free form for synthesis and tests. The parser sets the span of the whole
     *  constraint entry — {@code name: expr} or {@code name ( ... )}, closing paren
     *  inclusive (engine convention, verified via ProbeWireShapes). */
    public ConstraintDefinition(String name, Realization realization,
            @com.legend.base.Nullable ValueSpecification message,
            @com.legend.base.Nullable String enforcementLevel) {
        this(name, realization, message, enforcementLevel, null, null, null);
    }

    /** The common form: no ~message / ~enforcementLevel clauses. */
    public ConstraintDefinition(String name, Realization realization) {
        this(name, realization, null, null, null, null, null);
    }

    /** Position is excluded from equality — same contract as the value-spec records. */
    @Override
    public boolean equals(Object o) {
        return o instanceof ConstraintDefinition other
                && name.equals(other.name())
                && realization.equals(other.realization())
                && Objects.equals(message, other.message())
                && Objects.equals(enforcementLevel, other.enforcementLevel())
                && Objects.equals(externalId, other.externalId())
                && Objects.equals(owner, other.owner());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, realization, message, enforcementLevel, externalId, owner);
    }

    /** Convenience: the sugar (inline-predicate) form. */
    public ConstraintDefinition(String name, ValueSpecification expression) {
        this(name, new Realization.Inline(List.of(expression)), null, null);
    }

    /**
     * The inline predicate (sugar form). Valid only when the realization is
     * an {@link Realization.Inline}; a Door-4 function-ref binding has none.
     */
    public ValueSpecification expression() {
        if (realization instanceof Realization.Inline inl && inl.body().size() == 1) {
            return inl.body().get(0);
        }
        throw new IllegalStateException(
                "constraint '" + name + "' is a function-ref binding, not an inline predicate");
    }
}
