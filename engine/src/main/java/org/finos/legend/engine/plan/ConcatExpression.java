package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents string concatenation in SQL.
 * 
 * Compiles to dialect-specific syntax:
 * - DuckDB/SQLite/PostgreSQL: expr1 || expr2 || expr3
 * - MySQL: CONCAT(expr1, expr2, expr3)
 * 
 * @param parts The expressions being concatenated
 */
public record ConcatExpression(List<Expression> parts) implements Expression {
    
    public ConcatExpression {
        Objects.requireNonNull(parts, "Parts cannot be null");
        if (parts.size() < 2) {
            throw new IllegalArgumentException("Concatenation requires at least 2 parts");
        }
        parts = List.copyOf(parts);
    }
    
    public static ConcatExpression of(Expression... parts) {
        return new ConcatExpression(List.of(parts));
    }
    
    @Override
    public PureType type() {
        return PureType.STRING;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitConcat(this);
    }
    
    @Override
    public String toString() {
        return "CONCAT(" + parts + ")";
    }
}
