package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Represents a groupBy() function call.
 * 
 * Works on RelationExpression or VariableExpr (for let bindings).
 * 
 * Example: ->groupBy([{p | $p.department}], [{p | $p.salary}], ['totalSalary'])
 * 
 * @param source         The source expression (RelationExpression or
 *                       VariableExpr)
 * @param groupByColumns The column lambda expressions to group by
 * @param aggregations   The aggregation lambda expressions
 * @param aliases        Optional aliases for the aggregation results
 */
public record GroupByExpression(
        PureExpression source,
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
