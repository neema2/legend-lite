package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a collection operation with lambda in the IR.
 * 
 * Maps Pure collection functions:
 * - map(collection, x|expr)
 * - filter(collection, x|predicate)
 * - fold(collection, acc, elem|body, init)
 * - flatten(collection)
 */
public record CollectionExpression(
        CollectionFunction function,
        Expression source,
        String lambdaParam,
        String lambdaParam2, // For fold (accumulator)
        Expression lambdaBody,
        Expression initialValue // For fold
) implements Expression {

    public enum CollectionFunction {
        MAP,
        FILTER,
        FOLD,
        FLATTEN
    }

    public CollectionExpression {
        Objects.requireNonNull(function, "Function cannot be null");
        Objects.requireNonNull(source, "Source cannot be null");
    }

    /**
     * Creates a map operation: map(arr, x -> expr)
     */
    public static CollectionExpression map(Expression source, String param, Expression body) {
        return new CollectionExpression(CollectionFunction.MAP, source, param, null, body, null);
    }

    /**
     * Creates a filter operation: filter(arr, x -> predicate)
     */
    public static CollectionExpression filter(Expression source, String param, Expression body) {
        return new CollectionExpression(CollectionFunction.FILTER, source, param, null, body, null);
    }

    /**
     * Creates a fold operation: fold(arr, acc, elem -> body, init)
     */
    public static CollectionExpression fold(Expression source, String accParam, String elemParam,
            Expression body, Expression initialValue) {
        return new CollectionExpression(CollectionFunction.FOLD, source, elemParam, accParam, body, initialValue);
    }

    /**
     * Creates a flatten operation: flatten(arr)
     */
    public static CollectionExpression flatten(Expression source) {
        return new CollectionExpression(CollectionFunction.FLATTEN, source, null, null, null, null);
    }

    @Override
    public GenericType type() {
        return switch (function) {
            case MAP -> {
                if (lambdaBody == null) throw new IllegalStateException("MAP requires a lambda body");
                yield GenericType.listOf(lambdaBody.type());
            }
            case FILTER -> source.type();
            case FOLD -> {
                if (initialValue == null) throw new IllegalStateException("FOLD requires an initial value");
                yield initialValue.type();
            }
            case FLATTEN -> {
                // flatten(List<List<T>>) → List<T>
                GenericType inner = source.type().elementType();
                yield inner.isList() ? inner : source.type();
            }
        };
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitCollectionCall(this);
    }
}
