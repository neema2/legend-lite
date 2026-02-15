package org.finos.legend.engine.plan;

/**
 * Represents a date arithmetic operation.
 * 
 * Pure syntax: adjust(date, 5, DurationUnit.DAYS)
 * SQL output: DATE_ADD(date, INTERVAL 5 DAY)
 * 
 * Adds or subtracts a duration from a date.
 * Negative amounts subtract from the date.
 */
public record DateAdjustExpression(
        Expression date,
        Expression amount,
        DurationUnit unit) implements Expression {

    @Override
    public GenericType type() {
        return date.type();
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
