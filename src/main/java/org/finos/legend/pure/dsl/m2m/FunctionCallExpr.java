package org.finos.legend.pure.dsl.m2m;

import java.util.List;
import java.util.Objects;

/**
 * Represents a function call in M2M expressions.
 * 
 * Legend Pure uses arrow syntax for chained calls:
 * <pre>
 * $src.firstName->toUpper()
 * $src.name->trim()->toLower()
 * $src.name->substring(0, 10)
 * </pre>
 * 
 * @param target The expression the function is called on
 * @param functionName The function name (e.g., "toUpper", "trim", "substring")
 * @param arguments Additional arguments (beyond the target)
 */
public record FunctionCallExpr(
        M2MExpression target,
        String functionName,
        List<M2MExpression> arguments
) implements M2MExpression {
    
    public FunctionCallExpr {
        Objects.requireNonNull(target, "Target cannot be null");
        Objects.requireNonNull(functionName, "Function name cannot be null");
        Objects.requireNonNull(arguments, "Arguments cannot be null");
        arguments = List.copyOf(arguments);
    }
    
    /**
     * Creates a function call with no additional arguments.
     * E.g., $src.firstName->toUpper()
     */
    public static FunctionCallExpr of(M2MExpression target, String functionName) {
        return new FunctionCallExpr(target, functionName, List.of());
    }
    
    /**
     * Creates a function call with additional arguments.
     * E.g., $src.name->substring(0, 10)
     */
    public static FunctionCallExpr of(M2MExpression target, String functionName, M2MExpression... args) {
        return new FunctionCallExpr(target, functionName, List.of(args));
    }
    
    /**
     * @return True if this is a no-arg function call like ->toUpper()
     */
    public boolean hasNoArguments() {
        return arguments.isEmpty();
    }
    
    @Override
    public <T> T accept(M2MExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public String toString() {
        if (arguments.isEmpty()) {
            return target + "->" + functionName + "()";
        } else {
            String args = String.join(", ", arguments.stream().map(Object::toString).toList());
            return target + "->" + functionName + "(" + args + ")";
        }
    }
}
