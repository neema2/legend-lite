package org.finos.legend.pure.dsl.m2m;

import java.util.Objects;

/**
 * Represents a single property mapping in an M2M mapping.
 * 
 * Example:
 * <pre>
 * fullName: $src.firstName + ' ' + $src.lastName
 * </pre>
 * 
 * @param propertyName The target property name
 * @param expression The M2M expression defining the property value
 */
public record M2MPropertyMapping(
        String propertyName,
        M2MExpression expression
) {
    public M2MPropertyMapping {
        Objects.requireNonNull(propertyName, "Property name cannot be null");
        Objects.requireNonNull(expression, "Expression cannot be null");
    }
    
    @Override
    public String toString() {
        return propertyName + ": " + expression;
    }
}
