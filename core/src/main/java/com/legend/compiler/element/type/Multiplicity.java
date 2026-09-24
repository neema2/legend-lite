package com.legend.compiler.element.type;

import java.util.Objects;

/**
 * Typed-layer multiplicity &mdash; the kinded counterpart of
 * {@link com.legend.protocol.Multiplicity}, used inside the compiled
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

    /**
     * Convert a parse-time multiplicity to the compiled form &mdash; the single
     * parser&rarr;type-model conversion point (shared by element compilation and
     * type-annotation resolution).
     */
    static Multiplicity from(com.legend.protocol.Multiplicity m) {
        return switch (m) {
            case com.legend.protocol.Multiplicity.Concrete c -> new Bounded(c.lowerBound(), c.upperBound());
            case com.legend.protocol.Multiplicity.Parameter p -> new Var(p.name());
        };
    }

    // ====================================================================
    // THE multiplicity algebra — ONE owner (multiplicity audit
    // docs/MULTIPLICITY_AUDIT_2026_08_20.md §1d: the arithmetic was
    // re-derived at four sites with four DIFFERENT Var fallbacks; that
    // divergence is the mechanism that regenerates stamp bugs).
    // ====================================================================

    /**
     * RANGE UNION &mdash; the widest multiplicity covering both operands:
     * {@code [min(a,c) .. max(b,d)]}, an unbounded upper absorbing. This is
     * the shared {@code m} of {@code if<T|m>}/branch joins and the covariant
     * accumulation of a shared multiplicity variable.
     *
     * <p><strong>The {@code Var} rule, written down once:</strong> the same
     * variable on both sides IS that variable (a generic body's branches both
     * stamped {@code [m]} stay {@code [m]}). A variable meeting anything else
     * cannot union statically and is LOUD &mdash; the four old copies each
     * silently returned a different operand here, which is exactly the
     * divergence this owner exists to end. (Post-G, {@link #requireBounded}
     * doctrine: resolved expressions are always bounded.)
     */
    static Multiplicity union(Multiplicity a, Multiplicity b) {
        if (a instanceof Bounded x && b instanceof Bounded y) {
            return new Bounded(Math.min(x.lower(), y.lower()),
                    x.upper() == null || y.upper() == null
                            ? null : Math.max(x.upper(), y.upper()));
        }
        if (a instanceof Var va && b instanceof Var vb
                && va.name().equals(vb.name())) {
            return a;
        }
        throw new IllegalStateException("cannot union multiplicities "
                + a.text() + " and " + b.text()
                + " (an unresolved variable met a different bound)");
    }

    /**
     * PATH PRODUCT &mdash; composition along a navigation path:
     * {@code [a..b] . [c..d] = [a*c .. b*d]} (an unbounded upper on either
     * side stays unbounded). A {@code [*]} hop makes everything after it
     * {@code [*]}; an optional hop makes the result optional.
     *
     * <p><strong>The {@code Var} rule:</strong> {@code [1]} is the product's
     * identity on EITHER side &mdash; {@code [n] . [1] = [n]} stays the
     * variable (audit §1e: the old copy returned the other operand, silently
     * DROPPING a variable source's cardinality across inlining). Any other
     * variable product cannot be computed statically and is LOUD.
     */
    static Multiplicity product(Multiplicity outer, Multiplicity inner) {
        if (outer instanceof Bounded a && inner instanceof Bounded b) {
            // a [0..0] side ANNIHILATES (zero hops yield zero results)
            // — and beats the unbounded absorption: [0..0].[*] is [0..0]
            boolean zero = (a.upper() != null && a.upper() == 0)
                    || (b.upper() != null && b.upper() == 0);
            // exact arithmetic (audit D32): a raw int product wraps —
            // [65536].[65536] stamped [0..0], [0..MAX].[0..MAX] stamped
            // [0..1] — a fabricated cardinality, not a widening. A bound
            // past int range can only WEAKEN representably: an
            // overflowing upper ("at most N") becomes unbounded, an
            // overflowing lower ("at least N") saturates at MAX_VALUE.
            long lowerExact = (long) a.lower() * b.lower();
            Long upperExact = zero ? Long.valueOf(0)
                    : a.upper() == null || b.upper() == null
                            ? null : Long.valueOf(
                                    (long) a.upper() * b.upper());
            Integer upper = upperExact == null
                    || upperExact > Integer.MAX_VALUE
                            ? null : Integer.valueOf(upperExact.intValue());
            return new Bounded((int) Math.min(lowerExact,
                    Integer.MAX_VALUE), upper);
        }
        if (Bounded.ONE.equals(inner)) {
            return outer;
        }
        if (Bounded.ONE.equals(outer)) {
            return inner;
        }
        throw new IllegalStateException("cannot compose multiplicities "
                + outer.text() + " . " + inner.text()
                + " (an unresolved variable met a non-identity bound)");
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
    record Bounded(int lower, @com.legend.Nullable Integer upper) implements Multiplicity {

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

    /** One multiplicity ARGUMENT spelling ({@code Result<T|m>}'s {@code m},
     * {@code Result<X|1>}'s {@code 1}, {@code Column<Nil,Z|0..1>}'s
     * {@code 0..1}, {@code |*}): a bare name is a VARIABLE, everything else is
     * the ordinary bounds grammar. The one reading of the spelling — the
     * signature classifier and the value-position annotation both call it. */
    public static Multiplicity ofArgument(String spelling) {
        String s = spelling.strip();
        if ("*".equals(s)) {
            return Bounded.ZERO_MANY;
        }
        // the bounds grammar is fully decidable by SHAPE — no
        // exception-as-control-flow (ErrorShape guard): digits, or
        // digits..digits|*, else a VARIABLE name
        if (s.matches("[0-9]+")) {
            int n = Integer.parseInt(s);
            return new Bounded(n, n);
        }
        if (s.matches("[0-9]+\\.\\.([0-9]+|\\*)")) {
            int dots = s.indexOf("..");
            String up = s.substring(dots + 2);
            return new Bounded(Integer.parseInt(s.substring(0, dots)),
                    "*".equals(up) ? null : Integer.valueOf(up));
        }
        return new Var(s);
    }
}
