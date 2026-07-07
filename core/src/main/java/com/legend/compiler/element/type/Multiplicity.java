package com.legend.compiler.element.type;

import java.util.Objects;

/**
 * Typed-layer multiplicity &mdash; the kinded counterpart of
 * {@link com.legend.parser.Multiplicity}, used inside the compiled
 * {@link Type} hierarchy (Phase F output). Lives in {@code type/} so the
 * typed model never leaks the parser AST.
 *
 * <p>Two cases, mirroring {@code engine.m3.Multiplicity}:
 * <ul>
 *   <li>{@link Bounded}({@code lower, upper}) &mdash; fixed bounds;
 *       {@code upper == null} means unbounded ({@code *}).</li>
 *   <li>{@link Var}({@code name}) &mdash; a multiplicity <em>variable</em>
 *       (e.g. the {@code m} in {@code letFunction<T|m>(...): T[m]}).</li>
 * </ul>
 */
public sealed interface Multiplicity permits Multiplicity.Bounded, Multiplicity.Var {

    /**
     * Convert a parse-time multiplicity to the compiled form &mdash; the single
     * parser&rarr;type-model conversion point (shared by element compilation and
     * type-annotation resolution).
     */
    /**
     * Whether this multiplicity admits more than one value. THE single
     * implementation (an audit found five divergent copies).
     *
     * <p><strong>The {@code Var} decision, written down:</strong> an unbound
     * multiplicity variable is NOT "many" — checkers ask this question about
     * UNRESOLVED signatures, where treating {@code m} as many would
     * misclassify {@code T[m]} parameters (fold's accumulator, match's
     * branches). Post-G layers (lowering, exec) must never see a {@code Var}
     * at all — a resolved expression's multiplicity is always bounded — so
     * they guard with {@link #requireBounded} rather than silently treating
     * vars as many (which two of the five copies did).
     */
    default boolean isMany() {
        return this instanceof Bounded b && (b.upper() == null || b.upper() > 1);
    }

    /**
     * Post-G invariant guard: resolved expressions carry only {@link Bounded}
     * multiplicities; a surviving {@link Var} is a type-checker bug.
     */
    default Bounded requireBounded(String where) {
        if (this instanceof Bounded b) {
            return b;
        }
        throw new IllegalStateException(
                "unresolved multiplicity variable reached " + where + ": " + this);
    }

    static Multiplicity from(com.legend.parser.Multiplicity m) {
        return switch (m) {
            case com.legend.parser.Multiplicity.Concrete c -> new Bounded(c.lowerBound(), c.upperBound());
            case com.legend.parser.Multiplicity.Parameter p -> new Var(p.name());
        };
    }


    /** Source-style rendering, e.g. {@code [1]}, {@code [0..1]}, {@code [*]}, {@code [m]}. */
    String text();

    /**
     * Fixed-bound multiplicity, e.g. {@code [1]}, {@code [0..1]}, {@code [*]},
     * {@code [1..*]}, {@code [3..7]}.
     *
     * @param lower minimum cardinality, {@code >= 0}
     * @param upper maximum cardinality, {@code null} for unbounded ({@code *});
     *              otherwise {@code >= lower}
     */
    record Bounded(int lower, Integer upper) implements Multiplicity {

        public Bounded {
            if (lower < 0) {
                throw new IllegalArgumentException("lower must be >= 0, got " + lower);
            }
            if (upper != null && upper < lower) {
                throw new IllegalArgumentException(
                        "upper (" + upper + ") must be >= lower (" + lower + ")");
            }
        }

        /** {@code [1]} &mdash; exactly one. */
        public static final Bounded ONE = new Bounded(1, 1);

        /** {@code [0..1]} &mdash; zero or one (optional). */
        public static final Bounded ZERO_ONE = new Bounded(0, 1);

        /** {@code [1..*]} &mdash; one or more. */
        public static final Bounded ONE_MANY = new Bounded(1, null);

        /** {@code [*]} &mdash; zero or more. */
        public static final Bounded ZERO_MANY = new Bounded(0, null);

        /** {@code true} iff the upper bound is unbounded ({@code *}). */
        public boolean isUnbounded() {
            return upper == null;
        }

        /** {@code true} iff this multiplicity admits at most one value. */
        public boolean isToOne() {
            return upper != null && upper == 1;
        }

        @Override
        public String text() {
            if (upper == null) return lower == 0 ? "[*]" : "[" + lower + "..*]";
            if (lower == upper.intValue()) return "[" + lower + "]";
            return "[" + lower + ".." + upper + "]";
        }
    }

    /**
     * Multiplicity variable, e.g. the {@code m} in
     * {@code letFunction<T|m>(...): T[m]}.
     *
     * @param name parameter name, non-null, non-empty
     */
    record Var(String name) implements Multiplicity {
        public Var {
            Objects.requireNonNull(name, "name");
            if (name.isEmpty()) {
                throw new IllegalArgumentException("multiplicity parameter name must be non-empty");
            }
        }

        @Override
        public String text() {
            return "[" + name + "]";
        }
    }
}
