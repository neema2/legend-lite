package com.gs.legend.model.m3;

import java.util.Objects;

/**
 * Multiplicity annotation on a property, parameter, or function return type:
 * the {@code [1]}, {@code [*]}, {@code [0..1]}, {@code [1..*]} suffix in Pure syntax,
 * plus multiplicity variables like {@code m} (native function signatures only).
 *
 * <p>Sealed over two variants:
 * <ul>
 *   <li>{@link Bounded} — concrete lower/upper bounds (the common case). Upper bound
 *       {@code null} means unbounded ({@code *}).</li>
 *   <li>{@link Var} — multiplicity variable like {@code m} in
 *       {@code sort<T|m>(col:T[m]):T[m]}. Only appears in native function signatures
 *       and is bound during overload resolution.</li>
 * </ul>
 *
 * <p>Introduced as a sealed interface in Phase B 2.5b.3.5 to absorb the parallel
 * {@code compiler.Mult} type used for native signature multiplicity variables.
 * Accessor methods like {@link #lowerBound()} / {@link #upperBound()} work for
 * {@link Bounded} and throw for {@link Var} — callers that might see {@code Var}
 * should pattern-match explicitly.
 */
public sealed interface Multiplicity permits Multiplicity.Bounded, Multiplicity.Var {

    /** Single required value {@code [1]}. */
    Multiplicity ONE = new Bounded(1, 1);

    /** Optional single value {@code [0..1]}. */
    Multiplicity ZERO_ONE = new Bounded(0, 1);

    /** Alias for {@link #ZERO_ONE}. */
    Multiplicity ZERO_OR_ONE = ZERO_ONE;

    /** Zero or more {@code [*]}. */
    Multiplicity MANY = new Bounded(0, null);

    /** One or more {@code [1..*]}. */
    Multiplicity ONE_MANY = new Bounded(1, null);

    // -------- Accessors (default methods; throw for Var) --------

    /** Lower bound for {@link Bounded}; throws for {@link Var}. */
    default int lowerBound() {
        if (this instanceof Bounded b) return b.lowerBound;
        throw new UnsupportedOperationException(
                "Variable multiplicity '" + this + "' has no numeric lower bound");
    }

    /** Upper bound for {@link Bounded} ({@code null} = unbounded); throws for {@link Var}. */
    default Integer upperBound() {
        if (this instanceof Bounded b) return b.upperBound;
        throw new UnsupportedOperationException(
                "Variable multiplicity '" + this + "' has no numeric upper bound");
    }

    default boolean isRequired() {
        return this instanceof Bounded b && b.lowerBound >= 1;
    }

    default boolean isUnbounded() {
        return this instanceof Bounded b && b.upperBound == null;
    }

    default boolean isSingular() {
        return this instanceof Bounded b && b.upperBound != null && b.upperBound == 1;
    }

    /** {@code true} if this multiplicity allows multiple values (upper unbounded or &gt; 1). */
    default boolean isMany() {
        return this instanceof Bounded b && (b.upperBound == null || b.upperBound > 1);
    }

    // -------- Variants --------

    /**
     * Concrete multiplicity with lower and upper bounds.
     *
     * @param lowerBound Minimum cardinality (0 = optional, 1+ = required)
     * @param upperBound Maximum cardinality ({@code null} = unbounded)
     */
    record Bounded(int lowerBound, Integer upperBound) implements Multiplicity {
        public Bounded {
            if (lowerBound < 0) {
                throw new IllegalArgumentException("Lower bound cannot be negative");
            }
            if (upperBound != null && upperBound < lowerBound) {
                throw new IllegalArgumentException("Upper bound cannot be less than lower bound");
            }
        }

        @Override
        public String toString() {
            if (upperBound == null) {
                return lowerBound == 0 ? "[*]" : "[" + lowerBound + "..*]";
            }
            if (lowerBound == upperBound) {
                return "[" + lowerBound + "]";
            }
            return "[" + lowerBound + ".." + upperBound + "]";
        }
    }

    /**
     * Multiplicity variable (native function signature only). Bound during overload
     * resolution — e.g., {@code m} in {@code sort<T|m>(col:T[m]):T[m]} binds to the
     * multiplicity of the call-site's {@code col} argument.
     */
    record Var(String name) implements Multiplicity {
        public Var {
            Objects.requireNonNull(name, "Multiplicity variable name cannot be null");
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
