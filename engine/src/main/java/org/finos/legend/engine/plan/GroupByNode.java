package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a GROUP BY operation in the logical plan.
 * Produces SQL GROUP BY clause with aggregate projections.
 * 
 * Example Pure:
 * ->groupBy(~department, ~totalSalary : x|$x.salary : y|$y->sum())
 * 
 * SQL output:
 * SELECT department, SUM(salary) AS totalSalary
 * FROM ... GROUP BY department
 * 
 * For bi-variate aggregates:
 * ->groupBy(~department, ~correl : x|$x.salary->corr($x.years))
 * Produces: SELECT department, CORR(salary, years) AS correl
 */
public record GroupByNode(
        RelationNode source,
        List<String> groupingColumns,
        List<AggregateProjection> aggregations) implements RelationNode {

    /**
     * Represents a single aggregate projection in the GROUP BY result.
     * Maps the Pure syntax: ~alias : x|$x.sourceCol : y|$y->aggFunc()
     * 
     * For bi-variate functions like CORR, COVAR_SAMP:
     * ~alias : x|$x.col1->corr($x.col2)
     * 
     * For ordered-set aggregates like PERCENTILE_CONT:
     * ~alias : x|$x.col->percentileCont(0.5)
     * 
     * For string aggregation with separator:
     * ~alias : x|$x.col->joinStrings(':')
     * 
     * @param alias           Output column name
     * @param sourceColumn    First column being aggregated
     * @param secondColumn    Second column (for bi-variate functions like CORR)
     * @param function        The aggregate function (SUM, COUNT, CORR, etc.)
     * @param percentileValue The percentile value for PERCENTILE_CONT/DISC
     *                        (0.0-1.0)
     * @param separator       The separator string for STRING_AGG
     */
    public record AggregateProjection(
            String alias,
            String sourceColumn,
            String secondColumn,
            AggregateExpression.AggregateFunction function,
            Double percentileValue,
            String separator) {

        /**
         * Constructor for single-column aggregates.
         */
        public AggregateProjection(String alias, String sourceColumn,
                AggregateExpression.AggregateFunction function) {
            this(alias, sourceColumn, null, function, null, null);
        }

        /**
         * Constructor for bi-variate aggregates.
         */
        public AggregateProjection(String alias, String sourceColumn, String secondColumn,
                AggregateExpression.AggregateFunction function) {
            this(alias, sourceColumn, secondColumn, function, null, null);
        }

        /**
         * Constructor with percentile value (for PERCENTILE_CONT/DISC).
         */
        public AggregateProjection(String alias, String sourceColumn, String secondColumn,
                AggregateExpression.AggregateFunction function, Double percentileValue) {
            this(alias, sourceColumn, secondColumn, function, percentileValue, null);
        }

        public AggregateProjection {
            Objects.requireNonNull(alias, "Alias cannot be null");
            Objects.requireNonNull(sourceColumn, "Source column cannot be null");
            Objects.requireNonNull(function, "Aggregate function cannot be null");
            if (function.isBivariate() && secondColumn == null) {
                throw new IllegalArgumentException(function.name() + " requires two columns");
            }
            if (isPercentileFunction(function) && percentileValue == null) {
                throw new IllegalArgumentException(function.name() + " requires a percentile value");
            }
            if (function == AggregateExpression.AggregateFunction.STRING_AGG && separator == null) {
                throw new IllegalArgumentException("STRING_AGG requires a separator");
            }
        }

        private static boolean isPercentileFunction(AggregateExpression.AggregateFunction function) {
            return function == AggregateExpression.AggregateFunction.PERCENTILE_CONT
                    || function == AggregateExpression.AggregateFunction.PERCENTILE_DISC;
        }

        /**
         * Returns true if this is a bi-variate aggregate (e.g., CORR, COVAR).
         */
        public boolean isBivariate() {
            return secondColumn != null;
        }

        /**
         * Returns true if this is a percentile function.
         */
        public boolean isPercentile() {
            return isPercentileFunction(function);
        }

        /**
         * Returns true if this is a string aggregation function.
         */
        public boolean isStringAgg() {
            return function == AggregateExpression.AggregateFunction.STRING_AGG;
        }

        /**
         * Returns the second column if present.
         */
        public Optional<String> optionalSecondColumn() {
            return Optional.ofNullable(secondColumn);
        }

        /**
         * Returns the percentile value if present.
         */
        public Optional<Double> optionalPercentileValue() {
            return Optional.ofNullable(percentileValue);
        }

        /**
         * Returns the separator if present.
         */
        public Optional<String> optionalSeparator() {
            return Optional.ofNullable(separator);
        }
    }

    public GroupByNode {
        Objects.requireNonNull(source, "Source node cannot be null");
        Objects.requireNonNull(groupingColumns, "Grouping columns cannot be null");
        Objects.requireNonNull(aggregations, "Aggregations cannot be null");
        if (groupingColumns.isEmpty()) {
            throw new IllegalArgumentException("At least one grouping column is required");
        }
        if (aggregations.isEmpty()) {
            throw new IllegalArgumentException("At least one aggregation is required");
        }
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
