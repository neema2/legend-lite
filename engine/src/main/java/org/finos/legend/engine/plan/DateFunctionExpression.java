package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a date extraction function in the logical plan.
 * Maps to SQL EXTRACT(part FROM column) syntax.
 * 
 * Example Pure: $e.birthDate->year()
 * SQL output: EXTRACT(YEAR FROM "birthDate")
 */
public record DateFunctionExpression(
        DateFunction function,
        Expression argument) implements Expression {

    /**
     * Supported date extraction functions.
     */
    public enum DateFunction {
        YEAR("YEAR"),
        MONTH("MONTH"),
        DAY("DAY"),
        HOUR("HOUR"),
        MINUTE("MINUTE"),
        SECOND("SECOND"),
        DAY_OF_WEEK("DOW"), // DuckDB uses DOW for day of week
        DAY_OF_YEAR("DOY"), // DuckDB uses DOY for day of year
        WEEK_OF_YEAR("WEEK"),
        QUARTER("QUARTER");

        private final String sql;

        DateFunction(String sql) {
            this.sql = sql;
        }

        public String sql() {
            return sql;
        }
    }

    public DateFunctionExpression {
        Objects.requireNonNull(function, "Date function cannot be null");
        Objects.requireNonNull(argument, "Argument cannot be null");
    }

    @Override
    public PureType type() {
        return PureType.INTEGER;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
