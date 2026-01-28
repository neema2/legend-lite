package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Represents a groupBy() function call.
 * 
 * Only works on RelationExpression (from project() or another Relation
 * operation).
 * This is enforced at compile time!
 * 
 * Example: ->groupBy([{p | $p.department}], [{p | $p.salary}], ['totalSalary'])
 * 
 * @param source         The RELATION expression being grouped (compile-time
 *                       enforced!)
 * @param groupByColumns The column lambda expressions to group by
 * @param aggregations   The aggregation lambda expressions
 * @param aliases        Optional aliases for the aggregation results
 */
public record GroupByExpression(
        RelationExpression source,
        List<LambdaExpression> groupByColumns,
        List<LambdaExpression> aggregations,
        List<String> aliases) implements RelationExpression {
    public GroupByExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(groupByColumns, "GroupBy columns cannot be null");
        Objects.requireNonNull(aggregations, "Aggregations cannot be null");
        Objects.requireNonNull(aliases, "Aliases cannot be null");

        if (groupByColumns.isEmpty()) {
            throw new IllegalArgumentException("At least one groupBy column is required");
        }
        if (aggregations.isEmpty()) {
            throw new IllegalArgumentException("At least one aggregation is required");
        }
    }
}
