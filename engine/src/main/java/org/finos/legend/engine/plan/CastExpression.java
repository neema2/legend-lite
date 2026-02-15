package org.finos.legend.engine.plan;

/**
 * CAST expression: CAST(expr AS targetType)
 * Uses PureType instead of SQL strings — the SQL generator maps PureType to dialect-specific SQL.
 * 
 * @param source     The expression to cast
 * @param targetType The Pure type to cast to
 * @param isArray    If true, cast to array type (e.g., INTEGER[])
 */
public record CastExpression(Expression source, GenericType targetType, boolean isArray) implements Expression {

    /**
     * Convenience constructor for scalar (non-array) casts.
     */
    public CastExpression(Expression source, GenericType targetType) {
        this(source, targetType, false);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public GenericType type() {
        return isArray ? GenericType.LIST_ANY() : targetType;
    }
}
