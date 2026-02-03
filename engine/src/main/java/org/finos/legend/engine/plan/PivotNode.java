package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * IR node representing a PIVOT operation.
 * 
 * Maps to DuckDB:
 * PIVOT (source) ON pivotColumn USING agg(valueColumn) AS aggName
 */
public record PivotNode(
        RelationNode source,
        List<String> pivotColumns,
        List<AggregateSpec> aggregates,
        List<String> staticValues // Optional IN clause for static pivot
) implements RelationNode {

    /**
     * Aggregate specification for pivot.
     * Supports both column references and computed expressions.
     */
    public record AggregateSpec(
            String name, // Output column name suffix
            String valueColumn, // Column to aggregate (null for expressions)
            String valueExpression, // Expression string for computed values (null for columns)
            String aggFunction // Aggregate function (SUM, COUNT, etc.)
    ) {
        public AggregateSpec {
            Objects.requireNonNull(name, "Aggregate name cannot be null");
            Objects.requireNonNull(aggFunction, "Aggregate function cannot be null");
        }

        public static AggregateSpec column(String name, String column, String aggFunction) {
            return new AggregateSpec(name, column, null, aggFunction);
        }

        public static AggregateSpec expression(String name, String expression, String aggFunction) {
            return new AggregateSpec(name, null, expression, aggFunction);
        }

        public boolean isColumnBased() {
            return valueColumn != null;
        }
    }

    public PivotNode {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(pivotColumns, "Pivot columns cannot be null");
        if (pivotColumns.isEmpty()) {
            throw new IllegalArgumentException("At least one pivot column required");
        }
        Objects.requireNonNull(aggregates, "Aggregates cannot be null");
        if (aggregates.isEmpty()) {
            throw new IllegalArgumentException("At least one aggregate required");
        }
    }

    /**
     * Creates a dynamic pivot (columns determined at runtime).
     */
    public static PivotNode dynamic(RelationNode source, List<String> pivotColumns,
            List<AggregateSpec> aggregates) {
        return new PivotNode(source, pivotColumns, aggregates, List.of());
    }

    /**
     * Creates a static pivot with explicit column values.
     */
    public static PivotNode withValues(RelationNode source, List<String> pivotColumns,
            List<String> staticValues, List<AggregateSpec> aggregates) {
        return new PivotNode(source, pivotColumns, aggregates, staticValues);
    }

    /**
     * Returns true if this is a static pivot with explicit values.
     */
    public boolean isStatic() {
        return staticValues != null && !staticValues.isEmpty();
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
