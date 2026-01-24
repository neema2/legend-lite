package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a SQL collection operation with lambda.
 * 
 * Maps Pure collection functions to DuckDB equivalents:
 * - map() → list_transform(arr, lambda x: ...)
 * - filter() → list_filter(arr, lambda x: ...)
 * - fold() → list_reduce(arr, lambda acc, x: ..., init)
 * - flatten() → flatten(arr)
 */
public record SqlCollectionCall(
        CollectionFunction function,
        Expression source,
        String lambdaParam,
        String lambdaParam2, // For fold (accumulator)
        Expression lambdaBody,
        Expression initialValue // For fold
) implements Expression {

    public enum CollectionFunction {
        MAP, // list_transform
        FILTER, // list_filter
        FOLD, // list_reduce
        FLATTEN // flatten
    }

    public SqlCollectionCall {
        Objects.requireNonNull(function, "Function cannot be null");
        Objects.requireNonNull(source, "Source cannot be null");
    }

    /**
     * Creates a map operation: list_transform(arr, lambda x: expr)
     */
    public static SqlCollectionCall map(Expression source, String param, Expression body) {
        return new SqlCollectionCall(CollectionFunction.MAP, source, param, null, body, null);
    }

    /**
     * Creates a filter operation: list_filter(arr, lambda x: expr)
     */
    public static SqlCollectionCall filter(Expression source, String param, Expression body) {
        return new SqlCollectionCall(CollectionFunction.FILTER, source, param, null, body, null);
    }

    /**
     * Creates a fold operation: list_reduce(arr, lambda acc, x: expr, init)
     */
    public static SqlCollectionCall fold(Expression source, String accParam, String elemParam,
            Expression body, Expression initialValue) {
        return new SqlCollectionCall(CollectionFunction.FOLD, source, elemParam, accParam, body, initialValue);
    }

    /**
     * Creates a flatten operation: flatten(arr)
     */
    public static SqlCollectionCall flatten(Expression source) {
        return new SqlCollectionCall(CollectionFunction.FLATTEN, source, null, null, null, null);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitCollectionCall(this);
    }
}
