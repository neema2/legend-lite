package org.finos.legend.engine.plan;

/**
 * CAST expression: CAST(expr AS targetType)
 * Used for type conversion, particularly for variant/JSON to typed array
 * conversion.
 */
public record CastExpression(Expression source, String targetType) implements Expression {

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public SqlType type() {
        // Return the target type as the expression type
        return switch (targetType.toUpperCase()) {
            case "VARCHAR", "TEXT" -> SqlType.VARCHAR;
            case "BIGINT", "INTEGER", "INT" -> SqlType.BIGINT;
            case "DOUBLE", "FLOAT", "REAL" -> SqlType.DOUBLE;
            case "BOOLEAN", "BOOL" -> SqlType.BOOLEAN;
            default -> SqlType.UNKNOWN;
        };
    }
}
