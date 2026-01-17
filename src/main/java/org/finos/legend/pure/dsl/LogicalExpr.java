package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Represents a logical expression combining multiple conditions.
 * 
 * Examples: $p.age > 25 && $p.lastName == 'Smith'
 *          $p.status == 'active' || $p.status == 'pending'
 * 
 * @param operator The logical operator
 * @param operands The operand expressions
 */
public record LogicalExpr(
        Operator operator,
        List<PureExpression> operands
) implements PureExpression {
    
    public enum Operator {
        AND("&&"),
        OR("||"),
        NOT("!");
        
        private final String symbol;
        
        Operator(String symbol) {
            this.symbol = symbol;
        }
        
        public String symbol() {
            return symbol;
        }
    }
    
    public LogicalExpr {
        Objects.requireNonNull(operator, "Operator cannot be null");
        Objects.requireNonNull(operands, "Operands cannot be null");
        operands = List.copyOf(operands);
    }
    
    public static LogicalExpr and(PureExpression left, PureExpression right) {
        return new LogicalExpr(Operator.AND, List.of(left, right));
    }
    
    public static LogicalExpr or(PureExpression left, PureExpression right) {
        return new LogicalExpr(Operator.OR, List.of(left, right));
    }
    
    public static LogicalExpr not(PureExpression expr) {
        return new LogicalExpr(Operator.NOT, List.of(expr));
    }
}
