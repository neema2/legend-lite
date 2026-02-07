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
        // Handle parameterized types like DECIMAL(38,18)
        String upper = targetType.toUpperCase();
        if (upper.startsWith("DECIMAL") || upper.startsWith("NUMERIC")) return SqlType.DECIMAL;
        return switch (upper) {
            case "VARCHAR", "TEXT" -> SqlType.VARCHAR;
            case "BIGINT", "INTEGER", "INT" -> SqlType.BIGINT;
            case "DOUBLE", "FLOAT", "REAL" -> SqlType.DOUBLE;
            case "BOOLEAN", "BOOL" -> SqlType.BOOLEAN;
            default -> SqlType.UNKNOWN;
        };
    }
}
