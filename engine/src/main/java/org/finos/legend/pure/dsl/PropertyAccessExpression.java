package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents property access on an object.
 * 
 * Example: $p.lastName, $person.age
 * 
 * @param source The source expression (usually a variable)
 * @param propertyName The property being accessed
 */
public record PropertyAccessExpression(
        PureExpression source,
        String propertyName
) implements PureExpression {
    public PropertyAccessExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(propertyName, "Property name cannot be null");
    }
}
