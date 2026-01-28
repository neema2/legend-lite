package org.finos.legend.engine.plan;

/**
 * Represents epoch conversion functions.
 * 
 * - fromEpochValue(seconds) → EPOCH_MS(seconds * 1000) or TO_TIMESTAMP(seconds)
 * - toEpochValue(date) → EPOCH(date) or DATEDIFF FROM 1970-01-01
 */
public record EpochExpression(
        Expression value,
        DurationUnit unit,
        Direction direction) implements Expression {

    public enum Direction {
        FROM_EPOCH, // fromEpochValue: seconds → DateTime
        TO_EPOCH // toEpochValue: DateTime → seconds
    }

    /**
     * Creates fromEpochValue with SECONDS unit (default).
     */
    public static EpochExpression fromEpoch(Expression seconds) {
        return new EpochExpression(seconds, DurationUnit.SECONDS, Direction.FROM_EPOCH);
    }

    /**
     * Creates fromEpochValue with specified unit.
     */
    public static EpochExpression fromEpoch(Expression value, DurationUnit unit) {
        return new EpochExpression(value, unit, Direction.FROM_EPOCH);
    }

    /**
     * Creates toEpochValue with SECONDS unit (default).
     */
    public static EpochExpression toEpoch(Expression date) {
        return new EpochExpression(date, DurationUnit.SECONDS, Direction.TO_EPOCH);
    }

    /**
     * Creates toEpochValue with specified unit.
     */
    public static EpochExpression toEpoch(Expression date, DurationUnit unit) {
        return new EpochExpression(date, unit, Direction.TO_EPOCH);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitEpochExpression(this);
    }

    @Override
    public SqlType type() {
        return direction == Direction.FROM_EPOCH ? SqlType.TIMESTAMP : SqlType.BIGINT;
    }
}
