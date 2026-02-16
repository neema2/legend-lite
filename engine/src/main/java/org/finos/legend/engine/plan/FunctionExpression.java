package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents a function call expression in the IR.
 * 
 * Stores Pure-semantic function names (e.g., "toUpper", "abs", "substring").
 * The SQL generator is responsible for translating these to dialect-specific SQL.
 * 
 * @param functionName The Pure function name
 * @param target       The primary expression the function operates on
 * @param arguments    Additional arguments (if any)
 * @param returnType   The Pure type returned by this function
 */
public record FunctionExpression(
        String functionName,
        Expression target,
        List<Expression> arguments,
        GenericType returnType) implements Expression {

    public FunctionExpression {
        Objects.requireNonNull(functionName, "Function name cannot be null");
        Objects.requireNonNull(arguments, "Arguments cannot be null");
        arguments = List.copyOf(arguments);
        if (returnType == null) {
            returnType = GenericType.Primitive.ANY;
        }
    }

    /**
     * Creates a function call with no additional arguments. Return type resolved eagerly from registry.
     */
    public static FunctionExpression of(String functionName) {
        GenericType resolved = PureFunctionRegistry.resolveReturnType(functionName, GenericType.Primitive.ANY, List.of());
        return new FunctionExpression(functionName, null, List.of(), resolved);
    }

    public static FunctionExpression of(String functionName, Expression target) {
        GenericType targetType = target != null ? target.type() : GenericType.Primitive.ANY;
        GenericType resolved = PureFunctionRegistry.resolveReturnType(functionName, targetType, List.of());
        return new FunctionExpression(functionName, target, List.of(), resolved);
    }

    /**
     * Creates a function call with type specified.
     */
    public static FunctionExpression of(String functionName, Expression target, GenericType returnType) {
        return new FunctionExpression(functionName, target, List.of(), returnType);
    }

    /**
     * Creates a function call with additional arguments. Return type resolved eagerly from registry.
     */
    public static FunctionExpression of(String functionName, Expression target, Expression... args) {
        GenericType targetType = target != null ? target.type() : GenericType.Primitive.ANY;
        List<GenericType> argTypes = java.util.Arrays.stream(args).map(Expression::type).toList();
        GenericType resolved = PureFunctionRegistry.resolveReturnType(functionName, targetType, argTypes);
        return new FunctionExpression(functionName, target, List.of(args), resolved);
    }

    /**
     * Creates a function call with arguments and type.
     */
    public static FunctionExpression of(String functionName, Expression target, GenericType returnType, Expression... args) {
        return new FunctionExpression(functionName, target, List.of(args), returnType);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitFunctionCall(this);
    }

    @Override
    public GenericType type() {
        return returnType;
    }

    @Override
    public String toString() {
        if (arguments.isEmpty()) {
            return functionName.toUpperCase() + "(" + target + ")";
        } else {
            return functionName.toUpperCase() + "(" + target + ", " + arguments + ")";
        }
    }
}
