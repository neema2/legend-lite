package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents min/max comparison between two or more values.
 * 
 * Maps Pure max(d1, d2) and min(d1, d2) to SQL GREATEST/LEAST functions.
 * 
 * Example Pure: max($e.date1, $e.date2)
 * SQL output (DuckDB): GREATEST(date1, date2)
 */
public record MinMaxExpression(
        MinMaxFunction function,
        List<Expression> arguments) implements Expression {

    public enum MinMaxFunction {
        MIN("LEAST"),
        MAX("GREATEST");

        private final String sql;

        MinMaxFunction(String sql) {
            this.sql = sql;
        }

        public String sql() {
            return sql;
        }
    }

    public MinMaxExpression {
        Objects.requireNonNull(function, "Function cannot be null");
        Objects.requireNonNull(arguments, "Arguments cannot be null");
        if (arguments.size() < 2) {
            throw new IllegalArgumentException("min/max requires at least 2 arguments");
        }
    }

    /**
     * Factory method for min of two values.
     */
    public static MinMaxExpression min(Expression left, Expression right) {
        return new MinMaxExpression(MinMaxFunction.MIN, List.of(left, right));
    }

    /**
     * Factory method for max of two values.
     */
    public static MinMaxExpression max(Expression left, Expression right) {
        return new MinMaxExpression(MinMaxFunction.MAX, List.of(left, right));
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
