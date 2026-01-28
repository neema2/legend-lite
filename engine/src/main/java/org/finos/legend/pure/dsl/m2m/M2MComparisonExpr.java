package org.finos.legend.pure.dsl.m2m;

import java.util.Objects;

/**
 * Represents a comparison expression in M2M mappings.
 * 
 * Used primarily in if() conditions and filters.
 * 
 * Examples:
 * <pre>
 * $src.age < 18
 * $src.status == 'ACTIVE'
 * $src.amount >= 100.0
 * </pre>
 * 
 * @param left The left operand
 * @param operator The comparison operator
 * @param right The right operand
 */
public record M2MComparisonExpr(
        M2MExpression left,
        Operator operator,
        M2MExpression right
) implements M2MExpression {
    
    public enum Operator {
        EQUALS("=="),
        NOT_EQUALS("!="),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUALS("<="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUALS(">=");
        
        private final String symbol;
        
        Operator(String symbol) {
            this.symbol = symbol;
        }
        
        public String symbol() {
            return symbol;
        }
        
        public static Operator fromSymbol(String symbol) {
            for (Operator op : values()) {
                if (op.symbol.equals(symbol)) {
                    return op;
                }
            }
            throw new IllegalArgumentException("Unknown comparison operator: " + symbol);
        }
    }
    
    public M2MComparisonExpr {
        Objects.requireNonNull(left, "Left operand cannot be null");
        Objects.requireNonNull(operator, "Operator cannot be null");
        Objects.requireNonNull(right, "Right operand cannot be null");
    }
    
    public static M2MComparisonExpr equals(M2MExpression left, M2MExpression right) {
        return new M2MComparisonExpr(left, Operator.EQUALS, right);
    }
    
    public static M2MComparisonExpr notEquals(M2MExpression left, M2MExpression right) {
        return new M2MComparisonExpr(left, Operator.NOT_EQUALS, right);
    }
    
    public static M2MComparisonExpr lessThan(M2MExpression left, M2MExpression right) {
        return new M2MComparisonExpr(left, Operator.LESS_THAN, right);
    }
    
    public static M2MComparisonExpr lessThanOrEquals(M2MExpression left, M2MExpression right) {
        return new M2MComparisonExpr(left, Operator.LESS_THAN_OR_EQUALS, right);
    }
    
    public static M2MComparisonExpr greaterThan(M2MExpression left, M2MExpression right) {
        return new M2MComparisonExpr(left, Operator.GREATER_THAN, right);
    }
    
    public static M2MComparisonExpr greaterThanOrEquals(M2MExpression left, M2MExpression right) {
        return new M2MComparisonExpr(left, Operator.GREATER_THAN_OR_EQUALS, right);
    }
    
    @Override
    public <T> T accept(M2MExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public String toString() {
        return left + " " + operator.symbol() + " " + right;
    }
}
