package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents a variable reference in Pure.
 * 
 * Example: $p, $x, $person
 * 
 * @param name The variable name (without the $ prefix)
 */
public record VariableExpr(String name) implements PureExpression {
    public VariableExpr {
        Objects.requireNonNull(name, "Variable name cannot be null");
    }
}
