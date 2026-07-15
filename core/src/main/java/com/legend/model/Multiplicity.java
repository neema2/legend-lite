package com.legend.model;

import java.util.Objects;

/**
 * Multiplicity annotation for typed Pure declarations &mdash; the
 * bracketed part after a type, e.g. {@code Integer[1]},
 * {@code String[0..1]}, {@code Person[*]}.
 *
 * <p>Lives at the top of the {@code parser} package because it is
 * shared between element declarations (property / parameter / return
 * multiplicities) and value specifications (lambda parameter
 * multiplicities). Was previously duplicated as
 * {@code parser.element.Multiplicity} (with {@code lower}/{@code upper}
 * field names) and {@code parser.spec.Multiplicity} (with
 * {@code lowerBound}/{@code upperBound}). This consolidation keeps the
 * engine-protocol-faithful {@code lowerBound}/{@code upperBound} names
 * and merges both APIs.
 *
 * <p>Two concrete cases:
 *
 * <ul>
 *   <li>{@link Concrete}({@code lowerBound, upperBound}) &mdash; a
 *       fixed-bound multiplicity. {@code upperBound == null} means
 *       unbounded ({@code *}); otherwise both bounds are integers.</li>
 *   <li>{@link Parameter}({@code name}) &mdash; a multiplicity
 *       <em>variable</em>, e.g. the {@code m} in
 *       {@code letFunction<T|m>(s: String[1], v: T[m]): T[m]}.
 *       Used pervasively in stdlib native function signatures
 *       ({@code letFunction}, {@code if}, {@code cast}, {@code match},
 *       {@code reverse}, {@code sort}, {@code map}, etc.).</li>
 * </ul>
 *
 * <h2>Why two variants instead of one record</h2>
 *
 * <p>Engine-protocol's public {@code Multiplicity} class has only
 * {@code (lowerBound, upperBound)} fields &mdash; no parameter case.
 * That works for the protocol because it is serialised <em>after</em>
 * compilation has resolved every multiplicity variable to a concrete
 * value. We are a parser building the pre-compilation AST, so we must
 * represent both source forms.
 *
 * <p>A single record with a nullable parameter name would compile, but
 * encodes a runtime invariant ("parameter set implies bounds are
 * meaningless, and vice versa") that the type system cannot enforce.
 * The sealed split makes the mutual exclusion structural.
 *
 * <p>Legend-pure's M3 metamodel takes the same shape: a
 * {@code Multiplicity} core-instance carries both numeric bounds
 * <em>and</em> a separate {@code multiplicityParameter} slot; an
 * instance has one populated and the other empty.
 */
public sealed interface Multiplicity permits Multiplicity.Concrete, Multiplicity.Parameter {

    /**
     * Fixed-bound multiplicity, e.g. {@code [1]}, {@code [0..1]},
     * {@code [*]}, {@code [1..*]}, {@code [3..7]}.
     *
     * @param lowerBound minimum cardinality, {@code >= 0}
     * @param upperBound maximum cardinality, {@code null} for
     *                   unbounded ({@code *}); otherwise {@code >= lowerBound}
     */
    record Concrete(int lowerBound, Integer upperBound) implements Multiplicity {

        public Concrete {
            if (lowerBound < 0) {
                throw new IllegalArgumentException(
                        "lowerBound must be >= 0, got " + lowerBound);
            }
            if (upperBound != null && upperBound < lowerBound) {
                throw new IllegalArgumentException(
                        "upperBound (" + upperBound + ") must be >= lowerBound ("
                        + lowerBound + ")");
            }
        }

        /** {@code [1]} &mdash; exactly one. */
        public static final Concrete PURE_ONE = new Concrete(1, 1);

        /** {@code [0..1]} &mdash; zero or one (optional). */
        public static final Concrete ZERO_ONE = new Concrete(0, 1);

        /** {@code [1..*]} &mdash; one or more. */
        public static final Concrete PURE_MANY = new Concrete(1, null);

        /** {@code [*]} &mdash; zero or more. */
        public static final Concrete ZERO_MANY = new Concrete(0, null);

        /** {@code true} iff the upper bound is unbounded ({@code *}). */
        public boolean isInfinite() {
            return upperBound == null;
        }

        /** {@code true} iff the multiplicity admits at most one value. */
        public boolean isToOne() {
            return upperBound != null && upperBound == 1;
        }

        @Override
        public String toString() {
            if (upperBound == null) return lowerBound == 0 ? "[*]" : "[" + lowerBound + "..*]";
            if (lowerBound == upperBound.intValue()) return "[" + lowerBound + "]";
            return "[" + lowerBound + ".." + upperBound + "]";
        }
    }

    /**
     * Multiplicity variable, e.g. the {@code m} in
     * {@code letFunction<T|m>(...): T[m]}. The {@code name} is the
     * source-level identifier.
     *
     * @param name parameter name, non-null, non-empty
     */
    record Parameter(String name) implements Multiplicity {
        public Parameter {
            Objects.requireNonNull(name, "name");
            if (name.isEmpty()) {
                throw new IllegalArgumentException("multiplicity parameter name must be non-empty");
            }
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
