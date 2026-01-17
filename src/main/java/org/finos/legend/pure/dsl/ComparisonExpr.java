package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents a comparison expression in Pure.
 * 
 * Example: $p.lastName == 'Smith', $p.age > 25
 * 
 * @param left The left operand
 * @param operator The comparison operator
 * @param right The right operand
 */
public record ComparisonExpr(
        PureExpression left,
        Operator operator,
        PureExpression right
) implements PureExpression {
    
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
            throw new IllegalArgumentException("Unknown operator: " + symbol);
        }
    }
    
    public ComparisonExpr {
        Objects.requireNonNull(left, "Left operand cannot be null");
        Objects.requireNonNull(operator, "Operator cannot be null");
        Objects.requireNonNull(right, "Right operand cannot be null");
    }
}
