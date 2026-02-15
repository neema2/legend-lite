package org.finos.legend.engine.plan;

/**
 * Represents date comparison functions at the day level.
 * 
 * - isOnDay(d1, d2) → DATE_TRUNC('day', d1) = DATE_TRUNC('day', d2)
 * - isAfterDay(d1, d2) → DATE_TRUNC('day', d1) > DATE_TRUNC('day', d2)
 * - isBeforeDay(d1, d2) → DATE_TRUNC('day', d1) < DATE_TRUNC('day', d2)
 * - isOnOrAfterDay(d1, d2) → d1 >= d2 OR isOnDay
 * - isOnOrBeforeDay(d1, d2) → d1 <= d2 OR isOnDay
 */
public record DateComparisonExpression(
        Expression left,
        Expression right,
        DateCompareOp operation) implements Expression {

    public enum DateCompareOp {
        IS_ON_DAY, // d1 and d2 are on same calendar day
        IS_AFTER_DAY, // d1 is strictly after d2 (at day level)
        IS_BEFORE_DAY, // d1 is strictly before d2 (at day level)
        IS_ON_OR_AFTER_DAY, // d1 >= d2 (at day level)
        IS_ON_OR_BEFORE_DAY // d1 <= d2 (at day level)
    }

    public static DateComparisonExpression isOnDay(Expression left, Expression right) {
        return new DateComparisonExpression(left, right, DateCompareOp.IS_ON_DAY);
    }

    public static DateComparisonExpression isAfterDay(Expression left, Expression right) {
        return new DateComparisonExpression(left, right, DateCompareOp.IS_AFTER_DAY);
    }

    public static DateComparisonExpression isBeforeDay(Expression left, Expression right) {
        return new DateComparisonExpression(left, right, DateCompareOp.IS_BEFORE_DAY);
    }

    public static DateComparisonExpression isOnOrAfterDay(Expression left, Expression right) {
        return new DateComparisonExpression(left, right, DateCompareOp.IS_ON_OR_AFTER_DAY);
    }

    public static DateComparisonExpression isOnOrBeforeDay(Expression left, Expression right) {
        return new DateComparisonExpression(left, right, DateCompareOp.IS_ON_OR_BEFORE_DAY);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitDateComparisonExpression(this);
    }

    @Override
    public GenericType type() {
        return GenericType.Primitive.BOOLEAN;
    }
}
