package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents an aggregate expression for use in GROUP BY operations.
 * Maps to SQL aggregate functions like SUM, COUNT, AVG, MIN, MAX.
 * 
 * Example Pure: ~totalSalary : x|$x.salary : y|$y->sum()
 */
public record AggregateExpression(
        AggregateFunction function,
        Expression argument) implements Expression {

    /**
     * Supported aggregate functions that can be pushed to SQL.
     */
    public enum AggregateFunction {
        SUM("SUM"),
        COUNT("COUNT"),
        AVG("AVG"),
        MIN("MIN"),
        MAX("MAX"),
        COUNT_DISTINCT("COUNT(DISTINCT"); // special handling needed

        private final String sql;

        AggregateFunction(String sql) {
            this.sql = sql;
        }

        public String sql() {
            return sql;
        }
    }

    public AggregateExpression {
        Objects.requireNonNull(function, "Aggregate function cannot be null");
        Objects.requireNonNull(argument, "Aggregate argument cannot be null");
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitAggregate(this);
    }
}
