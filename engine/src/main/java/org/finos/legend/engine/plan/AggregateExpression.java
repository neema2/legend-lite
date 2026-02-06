package org.finos.legend.engine.plan;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents an aggregate expression for use in GROUP BY operations.
 * Maps to SQL aggregate functions like SUM, COUNT, AVG, MIN, MAX.
 * 
 * Example Pure: ~totalSalary : x|$x.salary : y|$y->sum()
 * 
 * For bi-variate functions like CORR, COVAR_SAMP:
 * Example Pure: ~correl : x|$x.salary->corr($x.years)
 * Produces: CORR(salary, years)
 */
public record AggregateExpression(
        AggregateFunction function,
        Expression argument,
        Expression secondArgument) implements Expression {

    /**
     * Supported aggregate functions that can be pushed to SQL.
     */
    public enum AggregateFunction {
        SUM("SUM", false),
        COUNT("COUNT", false),
        AVG("AVG", false),
        MIN("MIN", false),
        MAX("MAX", false),
        COUNT_DISTINCT("COUNT(DISTINCT", false), // special handling needed
        // Statistical functions (single-column)
        STDDEV("STDDEV", false),
        STDDEV_SAMP("STDDEV_SAMP", false),
        STDDEV_POP("STDDEV_POP", false),
        VARIANCE("VARIANCE", false),
        VAR_SAMP("VAR_SAMP", false),
        VAR_POP("VAR_POP", false),
        MEDIAN("MEDIAN", false),
        // Two-argument statistical functions (bi-variate)
        CORR("CORR", true),
        COVAR_SAMP("COVAR_SAMP", true),
        COVAR_POP("COVAR_POP", true),
        // Percentile functions (ordered-set aggregates)
        PERCENTILE_CONT("PERCENTILE_CONT", false),
        PERCENTILE_DISC("PERCENTILE_DISC", false),
        // String aggregation
        STRING_AGG("STRING_AGG", false),
        // Mode (most frequent value)
        MODE("MODE", false);

        private final String sql;
        private final boolean bivariate;

        AggregateFunction(String sql, boolean bivariate) {
            this.sql = sql;
            this.bivariate = bivariate;
        }

        public String sql() {
            return sql;
        }

        public boolean isBivariate() {
            return bivariate;
        }
    }

    /**
     * Constructor for single-argument aggregates.
     */
    public AggregateExpression(AggregateFunction function, Expression argument) {
        this(function, argument, null);
    }

    public AggregateExpression {
        Objects.requireNonNull(function, "Aggregate function cannot be null");
        Objects.requireNonNull(argument, "Aggregate argument cannot be null");
        if (function.isBivariate() && secondArgument == null) {
            throw new IllegalArgumentException(function.name() + " requires two arguments");
        }
    }

    /**
     * Returns the second argument if present (for bi-variate functions).
     */
    public Optional<Expression> optionalSecondArgument() {
        return Optional.ofNullable(secondArgument);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitAggregate(this);
    }
}
