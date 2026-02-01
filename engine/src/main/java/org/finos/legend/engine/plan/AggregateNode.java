package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents an aggregate operation in the logical plan - aggregates an entire
 * relation into a single row without any grouping columns.
 * 
 * This is distinct from GroupByNode which requires at least one grouping
 * column.
 * 
 * Example Pure:
 * ->aggregate(~totalSalary : x|$x.salary : y|$y->sum())
 * 
 * SQL output:
 * SELECT SUM(salary) AS totalSalary FROM ...
 * (no GROUP BY clause)
 */
public record AggregateNode(
        RelationNode source,
        List<GroupByNode.AggregateProjection> aggregations) implements RelationNode {

    public AggregateNode {
        Objects.requireNonNull(source, "Source node cannot be null");
        Objects.requireNonNull(aggregations, "Aggregations cannot be null");
        if (aggregations.isEmpty()) {
            throw new IllegalArgumentException("At least one aggregation is required");
        }
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
