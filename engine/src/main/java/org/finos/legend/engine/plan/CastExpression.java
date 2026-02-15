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
    public PureType type() {
        // Handle parameterized types like DECIMAL(38,18)
        String upper = targetType.toUpperCase();
        if (upper.startsWith("DECIMAL") || upper.startsWith("NUMERIC")) return PureType.DECIMAL;
        return switch (upper) {
            case "VARCHAR", "TEXT" -> PureType.STRING;
            case "BIGINT", "INTEGER", "INT" -> PureType.INTEGER;
            case "DOUBLE", "FLOAT", "REAL" -> PureType.FLOAT;
            case "BOOLEAN", "BOOL" -> PureType.BOOLEAN;
            case "DATE" -> PureType.STRICT_DATE;
            case "TIMESTAMP" -> PureType.DATE_TIME;
            case "TIME" -> PureType.STRICT_TIME;
            default -> PureType.UNKNOWN;
        };
    }
}
