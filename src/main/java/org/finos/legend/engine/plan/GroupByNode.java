package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

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
 */
public record GroupByNode(
        RelationNode source,
        List<String> groupingColumns,
        List<AggregateProjection> aggregations) implements RelationNode {

    /**
     * Represents a single aggregate projection in the GROUP BY result.
     * Maps the Pure syntax: ~alias : x|$x.sourceCol : y|$y->aggFunc()
     * 
     * @param alias        Output column name
     * @param sourceColumn Column being aggregated
     * @param function     The aggregate function (SUM, COUNT, etc.)
     */
    public record AggregateProjection(
            String alias,
            String sourceColumn,
            AggregateExpression.AggregateFunction function) {
        public AggregateProjection {
            Objects.requireNonNull(alias, "Alias cannot be null");
            Objects.requireNonNull(sourceColumn, "Source column cannot be null");
            Objects.requireNonNull(function, "Aggregate function cannot be null");
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
