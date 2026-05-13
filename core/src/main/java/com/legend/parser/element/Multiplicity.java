package com.legend.parser.element;

import java.util.Objects;

/**
 * A parsed Pure multiplicity annotation &mdash; the {@code [...]} suffix on a
 * declared type.
 *
 * <p>Mirrors Pure's M3 grammar:
 * <pre>
 *   multiplicity         : '[' multiplicityArgument ']'
 *   multiplicityArgument : identifier
 *                        | ( (fromMultiplicity '..')? toMultiplicity )
 * </pre>
 *
 * <p>Two variants:
 * <ul>
 *   <li>{@link Concrete} &mdash; fixed bounds: {@code [1]}, {@code [0..1]},
 *       {@code [*]}, {@code [1..*]}. {@code upper == null} means unbounded
 *       ({@code *}).</li>
 *   <li>{@link Parameter} &mdash; an identifier referring to a multiplicity
 *       parameter declared in a generic function signature, e.g. {@code [m]}
 *       in {@code function f<|m>(x:Any[m]):Any[m]}.</li>
 * </ul>
 *
 * <p>Callers must pattern-match the sealed hierarchy &mdash; there is no
 * defaulting accessor that pretends a parameter multiplicity has bounds.
 */
public sealed interface Multiplicity {

    /**
     * Concrete multiplicity bounds. {@code upper == null} = unbounded
     * ({@code *}); otherwise {@code lower <= upper}.
     */
    record Concrete(int lower, Integer upper) implements Multiplicity {
        public Concrete {
            if (lower < 0) {
                throw new IllegalArgumentException("lower bound must be >= 0, got " + lower);
            }
            if (upper != null && upper < lower) {
                throw new IllegalArgumentException(
                        "upper bound " + upper + " must be >= lower bound " + lower);
            }
        }

        @Override
        public String toString() {
            if (upper == null) return lower == 0 ? "[*]" : "[" + lower + "..*]";
            if (lower == upper) return "[" + lower + "]";
            return "[" + lower + ".." + upper + "]";
        }
    }

    /**
     * A reference to a multiplicity parameter declared in an enclosing
     * function signature, e.g. {@code [m]} where {@code m} appears in
     * the function's {@code <|m>} declaration.
     */
    record Parameter(String name) implements Multiplicity {
        public Parameter {
            Objects.requireNonNull(name, "multiplicity parameter name cannot be null");
        }

        @Override
        public String toString() {
            return "[" + name + "]";
        }
    }

    // ========== Factories ==========

    /** {@code [n]} where {@code n} is a fixed integer. */
    static Multiplicity exactly(int n) {
        return new Concrete(n, n);
    }

    /** {@code [lower..upper]}; pass {@code null} for unbounded upper. */
    static Multiplicity range(int lower, Integer upper) {
        return new Concrete(lower, upper);
    }

    /** {@code [*]} &mdash; zero or more, unbounded. */
    static Multiplicity zeroMany() {
        return new Concrete(0, null);
    }

    /** {@code [m]} where {@code m} is a multiplicity parameter name. */
    static Multiplicity parameter(String name) {
        return new Parameter(name);
    }
}
