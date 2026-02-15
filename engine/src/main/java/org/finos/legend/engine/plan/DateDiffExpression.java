package org.finos.legend.engine.plan;

/**
 * Represents a date difference operation.
 * 
 * Pure syntax: dateDiff(d1, d2, DurationUnit.DAYS)
 * SQL output: DATE_DIFF('day', d1, d2)
 * 
 * Returns the number of units between two dates.
 */
public record DateDiffExpression(
        Expression date1,
        Expression date2,
        DurationUnit unit) implements Expression {

    @Override
    public GenericType type() {
        return GenericType.Primitive.INTEGER;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
