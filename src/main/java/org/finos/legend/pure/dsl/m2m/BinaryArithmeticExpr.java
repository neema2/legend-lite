package org.finos.legend.pure.dsl.m2m;

import java.util.Objects;

/**
 * Represents a binary arithmetic expression.
 * 
 * Examples:
 * <pre>
 * $src.price * $src.quantity
 * $src.total - $src.discount
 * </pre>
 * 
 * Note: String concatenation with + is handled by StringConcatExpr.
 * 
 * @param left The left operand
 * @param operator The operator
 * @param right The right operand
 */
public record BinaryArithmeticExpr(
        M2MExpression left,
        Operator operator,
        M2MExpression right
) implements M2MExpression {
    
    public enum Operator {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/");
        
        private final String symbol;
        
        Operator(String symbol) {
            this.symbol = symbol;
        }
        
        public String symbol() {
            return symbol;
        }
    }
    
    public BinaryArithmeticExpr {
        Objects.requireNonNull(left, "Left operand cannot be null");
        Objects.requireNonNull(operator, "Operator cannot be null");
        Objects.requireNonNull(right, "Right operand cannot be null");
    }
    
    public static BinaryArithmeticExpr add(M2MExpression left, M2MExpression right) {
        return new BinaryArithmeticExpr(left, Operator.ADD, right);
    }
    
    public static BinaryArithmeticExpr subtract(M2MExpression left, M2MExpression right) {
        return new BinaryArithmeticExpr(left, Operator.SUBTRACT, right);
    }
    
    public static BinaryArithmeticExpr multiply(M2MExpression left, M2MExpression right) {
        return new BinaryArithmeticExpr(left, Operator.MULTIPLY, right);
    }
    
    public static BinaryArithmeticExpr divide(M2MExpression left, M2MExpression right) {
        return new BinaryArithmeticExpr(left, Operator.DIVIDE, right);
    }
    
    @Override
    public <T> T accept(M2MExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public String toString() {
        return "(" + left + " " + operator.symbol() + " " + right + ")";
    }
}
