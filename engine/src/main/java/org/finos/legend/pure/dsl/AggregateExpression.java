package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Represents an aggregate() function call - aggregates an entire relation
 * into a single row without any grouping.
 * 
 * Example: ->aggregate(~idSum : x | $x.id : y | $y->plus())
 * Example: ->aggregate(~[sum1: x|$x.a : y|$y->plus(), sum2: x|$x.b :
 * y|$y->plus()])
 * 
 * This produces a single row with the aggregated values.
 * Unlike groupBy(), there are no grouping columns.
 *
 * @param source       The RELATION expression being aggregated
 * @param mapFunctions The map lambda expressions (e.g., x | $x.id) - gives the
 *                     column to aggregate
 * @param aggFunctions The aggregation lambda expressions (e.g., y | $y->plus())
 *                     - gives the aggregation
 * @param aliases      The names for the aggregation result columns
 */
public record AggregateExpression(
        RelationExpression source,
        List<LambdaExpression> mapFunctions,
        List<LambdaExpression> aggFunctions,
        List<String> aliases) implements RelationExpression {
    public AggregateExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(mapFunctions, "Map functions cannot be null");
        Objects.requireNonNull(aggFunctions, "Aggregation functions cannot be null");
        Objects.requireNonNull(aliases, "Aliases cannot be null");

        if (mapFunctions.isEmpty()) {
            throw new IllegalArgumentException("At least one map function is required");
        }
        if (mapFunctions.size() != aggFunctions.size()) {
            throw new IllegalArgumentException("Map functions and agg functions must have the same size");
        }
    }
}
