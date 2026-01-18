package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents a SQL function call.
 * 
 * Used for functions like UPPER(), LOWER(), TRIM(), etc.
 * 
 * @param functionName The SQL function name (lowercase)
 * @param target The primary expression the function operates on
 * @param arguments Additional arguments (if any)
 */
public record SqlFunctionCall(
        String functionName,
        Expression target,
        List<Expression> arguments
) implements Expression {
    
    public SqlFunctionCall {
        Objects.requireNonNull(functionName, "Function name cannot be null");
        Objects.requireNonNull(target, "Target cannot be null");
        Objects.requireNonNull(arguments, "Arguments cannot be null");
        arguments = List.copyOf(arguments);
    }
    
    /**
     * Creates a function call with no additional arguments.
     * E.g., UPPER(column)
     */
    public static SqlFunctionCall of(String functionName, Expression target) {
        return new SqlFunctionCall(functionName, target, List.of());
    }
    
    /**
     * Creates a function call with additional arguments.
     * E.g., SUBSTRING(column, 1, 10)
     */
    public static SqlFunctionCall of(String functionName, Expression target, Expression... args) {
        return new SqlFunctionCall(functionName, target, List.of(args));
    }
    
    /**
     * @return The SQL function name to use for this function
     */
    public String sqlFunctionName() {
        // Map Pure function names to SQL function names
        return switch (functionName) {
            case "toupper" -> "UPPER";
            case "tolower" -> "LOWER";
            case "trim" -> "TRIM";
            case "length" -> "LENGTH";
            case "abs" -> "ABS";
            case "round" -> "ROUND";
            case "ceiling" -> "CEIL";
            case "floor" -> "FLOOR";
            case "tostring" -> "CAST"; // Will need special handling
            case "tointeger" -> "CAST"; // Will need special handling
            default -> functionName.toUpperCase();
        };
    }
    
    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitFunctionCall(this);
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
