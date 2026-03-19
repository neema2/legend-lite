package com.gs.legend.ast;

/**
 * Variable reference in Pure expressions.
 *
 * <p>
 * Represents {@code $x} in Pure. The name does NOT include the {@code $}
 * prefix.
 *
 * <p>
 * Variables may optionally carry type information from type annotations
 * in lambda parameters (e.g., {@code {x:Integer[1] | ...}}).
 *
 * @param name         The variable name (without $ prefix)
 * @param typeName     Optional type name (e.g., "Integer", "String")
 * @param multiplicity Optional multiplicity (e.g., "1", "1..4", "*")
 */
public record Variable(
                String name,
                String typeName,
                String multiplicity) implements ValueSpecification {

        /** Convenience constructor for untyped variables. */
        public Variable(String name) {
                this(name, null, null);
        }
}
