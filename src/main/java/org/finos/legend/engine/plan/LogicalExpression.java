package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents a logical expression combining multiple conditions.
 * 
 * @param operator The logical operator (AND, OR, NOT)
 * @param operands The operand expressions
 */
public record LogicalExpression(
        LogicalOperator operator,
        List<Expression> operands
) implements Expression {
    
    public enum LogicalOperator {
        AND,
        OR,
        NOT
    }
    
    public LogicalExpression {
        Objects.requireNonNull(operator, "Operator cannot be null");
        Objects.requireNonNull(operands, "Operands cannot be null");
        
        // Ensure immutability
        operands = List.copyOf(operands);
        
        // Validate operand count
        if (operator == LogicalOperator.NOT && operands.size() != 1) {
            throw new IllegalArgumentException("NOT operator requires exactly 1 operand");
        }
        if (operator != LogicalOperator.NOT && operands.size() < 2) {
            throw new IllegalArgumentException(operator + " operator requires at least 2 operands");
        }
    }
    
    /**
     * Factory for AND expression.
     */
    public static LogicalExpression and(Expression... expressions) {
        return new LogicalExpression(LogicalOperator.AND, List.of(expressions));
    }
    
    /**
     * Factory for AND expression.
     */
    public static LogicalExpression and(List<Expression> expressions) {
        return new LogicalExpression(LogicalOperator.AND, expressions);
    }
    
    /**
     * Factory for OR expression.
     */
    public static LogicalExpression or(Expression... expressions) {
        return new LogicalExpression(LogicalOperator.OR, List.of(expressions));
    }
    
    /**
     * Factory for NOT expression.
     */
    public static LogicalExpression not(Expression expression) {
        return new LogicalExpression(LogicalOperator.NOT, List.of(expression));
    }
    
    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitLogical(this);
    }
    
    @Override
    public String toString() {
        if (operator == LogicalOperator.NOT) {
            return "NOT (" + operands.getFirst() + ")";
        }
        return "(" + String.join(" " + operator + " ", 
                operands.stream().map(Object::toString).toList()) + ")";
    }
}
