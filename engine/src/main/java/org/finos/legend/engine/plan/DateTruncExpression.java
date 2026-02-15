package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a date truncation function in the logical plan.
 * Maps to SQL DATE_TRUNC(part, column) syntax.
 * 
 * Example Pure: $e.date->firstDayOfMonth()
 * SQL output: DATE_TRUNC('month', column)
 */
public record DateTruncExpression(
        TruncPart part,
        Expression argument) implements Expression {

    /**
     * Supported date truncation parts.
     */
    public enum TruncPart {
        YEAR("year"),
        QUARTER("quarter"),
        MONTH("month"),
        WEEK("week"),
        DAY("day"),
        HOUR("hour"),
        MINUTE("minute"),
        SECOND("second");

        private final String sql;

        TruncPart(String sql) {
            this.sql = sql;
        }

        public String sql() {
            return sql;
        }
    }

    public DateTruncExpression {
        Objects.requireNonNull(part, "Trunc part cannot be null");
        Objects.requireNonNull(argument, "Argument cannot be null");
    }

    @Override
    public GenericType type() {
        return argument.type() != GenericType.Primitive.DEFERRED ? argument.type() : GenericType.Primitive.STRICT_DATE;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
