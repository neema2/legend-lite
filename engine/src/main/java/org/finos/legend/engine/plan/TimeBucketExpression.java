package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a time bucket operation in the logical plan.
 * 
 * Maps Pure timeBucket(date, quantity, unit) to SQL TIME_BUCKET or DATE_TRUNC
 * equivalent.
 * 
 * Example Pure: $e.eventDate->timeBucket(5, DurationUnit.DAYS)
 * SQL output (DuckDB): TIME_BUCKET(INTERVAL '5 days', eventDate)
 */
public record TimeBucketExpression(
        Expression dateExpression,
        Expression quantity,
        DurationUnit unit) implements Expression {

    public TimeBucketExpression {
        Objects.requireNonNull(dateExpression, "Date expression cannot be null");
        Objects.requireNonNull(quantity, "Quantity cannot be null");
        Objects.requireNonNull(unit, "Unit cannot be null");
    }

    /**
     * Factory method for creating a time bucket expression.
     */
    public static TimeBucketExpression of(Expression dateExpr, Expression quantity, DurationUnit unit) {
        return new TimeBucketExpression(dateExpr, quantity, unit);
    }

    @Override
    public GenericType type() {
        return dateExpression.type() != GenericType.Primitive.ANY ? dateExpression.type() : GenericType.Primitive.DATE_TIME;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
