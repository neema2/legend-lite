package org.finos.legend.engine.plan;

/**
 * Represents a DuckDB list_filter expression for filtering arrays/lists.
 * 
 * DuckDB syntax: list_filter(array, x -> condition)
 * 
 * Example:
 * list_filter([1,2,3,4], x -> x % 2 = 0) returns [2,4]
 */
public record ListFilterExpression(
        Expression source,
        String lambdaParameter,
        Expression condition) implements Expression {

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
