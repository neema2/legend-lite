package com.gs.legend.compiler;

import com.gs.legend.model.m3.Multiplicity;

/**
 * Pure multiplicity — the {@code [1]}, {@code [*]}, {@code [0..1]}, {@code [1..*]}
 * annotations on parameters and return types, plus multiplicity variables like {@code m}.
 *
 * <p>
 * Used in native function signatures parsed from Pure source strings.
 * {@link Fixed} wraps the existing {@link Multiplicity} record for concrete values.
 * {@link Var} represents multiplicity variables (e.g., {@code m} in {@code sort<T|m>(col:T[m]):T[m]}).
 */
public sealed interface Mult {

    /**
     * A concrete multiplicity: {@code [1]}, {@code [*]}, {@code [0..1]}, {@code [1..*]}.
     */
    record Fixed(Multiplicity value) implements Mult {
        @Override
        public String toString() {
            if (value.equals(Multiplicity.ONE)) return "1";
            if (value.equals(Multiplicity.MANY)) return "*";
            if (value.equals(Multiplicity.ZERO_ONE)) return "0..1";
            if (value.equals(Multiplicity.ONE_MANY)) return "1..*";
            return value.lowerBound() + ".." + (value.upperBound() == null ? "*" : value.upperBound());
        }
    }

    /**
     * A multiplicity variable like {@code m}, {@code n}. Appears in type parameter
     * declarations like {@code <T|m>} and in parameter multiplicities like {@code T[m]}.
     */
    record Var(String name) implements Mult {
        @Override
        public String toString() { return name; }
    }

    // ===== Constants =====

    Mult ONE = new Fixed(Multiplicity.ONE);
    Mult ZERO_MANY = new Fixed(Multiplicity.MANY);
    Mult ZERO_ONE = new Fixed(Multiplicity.ZERO_ONE);
    Mult ONE_MANY = new Fixed(Multiplicity.ONE_MANY);

    // ===== Parsing helper =====

    /**
     * Parse a multiplicity string from Pure syntax.
     */
    static Mult parse(String s) {
        return switch (s.trim()) {
            case "1" -> ONE;
            case "*" -> ZERO_MANY;
            case "0..1" -> ZERO_ONE;
            case "1..*" -> ONE_MANY;
            default -> new Var(s.trim());
        };
    }
}

