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
     * @param alias        Output column name
     * @param sourceColumn First column being aggregated
     * @param secondColumn Second column (for bi-variate functions like CORR)
     * @param function     The aggregate function (SUM, COUNT, CORR, etc.)
     */
    public record AggregateProjection(
            String alias,
            String sourceColumn,
            String secondColumn,
            AggregateExpression.AggregateFunction function) {

        /**
         * Constructor for single-column aggregates.
         */
        public AggregateProjection(String alias, String sourceColumn,
                AggregateExpression.AggregateFunction function) {
            this(alias, sourceColumn, null, function);
        }

        public AggregateProjection {
            Objects.requireNonNull(alias, "Alias cannot be null");
            Objects.requireNonNull(sourceColumn, "Source column cannot be null");
            Objects.requireNonNull(function, "Aggregate function cannot be null");
            if (function.isBivariate() && secondColumn == null) {
                throw new IllegalArgumentException(function.name() + " requires two columns");
            }
        }

        /**
         * Returns true if this is a bi-variate aggregate (e.g., CORR, COVAR).
         */
        public boolean isBivariate() {
            return secondColumn != null;
        }

        /**
         * Returns the second column if present.
         */
        public Optional<String> optionalSecondColumn() {
            return Optional.ofNullable(secondColumn);
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
