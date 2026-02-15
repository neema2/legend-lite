package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a comparison expression (e.g., column = 'value').
 * 
 * @param left The left operand
 * @param operator The comparison operator
 * @param right The right operand
 */
public record ComparisonExpression(
        Expression left,
        ComparisonOperator operator,
        Expression right
) implements Expression {
    
    public enum ComparisonOperator {
        EQUALS("="),
        NOT_EQUALS("<>"),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUALS("<="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUALS(">="),
        LIKE("LIKE"),
        IS_NULL("IS NULL"),
        IS_NOT_NULL("IS NOT NULL");
        
        private final String sql;
        
        ComparisonOperator(String sql) {
            this.sql = sql;
        }
        
        public String toSql() {
            return sql;
        }
    }
    
    public ComparisonExpression {
        Objects.requireNonNull(left, "Left operand cannot be null");
        Objects.requireNonNull(operator, "Operator cannot be null");
        // Right can be null for IS NULL / IS NOT NULL
    }
    
    /**
     * Factory for equals comparison.
     */
    public static ComparisonExpression equals(Expression left, Expression right) {
        return new ComparisonExpression(left, ComparisonOperator.EQUALS, right);
    }
    
    /**
     * Factory for column equals literal.
     */
    public static ComparisonExpression columnEquals(String tableAlias, String columnName, String value) {
        return equals(
                ColumnReference.of(tableAlias, columnName),
                Literal.string(value)
        );
    }
    
    /**
     * Factory for less than comparison.
     */
    public static ComparisonExpression lessThan(Expression left, Expression right) {
        return new ComparisonExpression(left, ComparisonOperator.LESS_THAN, right);
    }
    
    /**
     * Factory for greater than comparison.
     */
    public static ComparisonExpression greaterThan(Expression left, Expression right) {
        return new ComparisonExpression(left, ComparisonOperator.GREATER_THAN, right);
    }
    
    @Override
    public PureType type() {
        return PureType.BOOLEAN;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitComparison(this);
    }
    
    @Override
    public String toString() {
        if (operator == ComparisonOperator.IS_NULL || operator == ComparisonOperator.IS_NOT_NULL) {
            return left + " " + operator.toSql();
        }
        return left + " " + operator.toSql() + " " + right;
    }
}
