package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents a cast expression: expr->cast(@Type)
 * 
 * Casts the source expression to the specified type for type-safe operations.
 * 
 * @param source     The expression to cast
 * @param targetType The target type name (e.g., "Integer", "String", "Float")
 */
public record CastExpression(
        PureExpression source,
        String targetType) implements PureExpression {

    public CastExpression {
        Objects.requireNonNull(source, "Cast source cannot be null");
        Objects.requireNonNull(targetType, "Cast target type cannot be null");
    }

    @Override
    public String toString() {
        return source + "->cast(@" + targetType + ")";
    }
}
