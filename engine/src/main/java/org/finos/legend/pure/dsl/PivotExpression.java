package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Represents a pivot expression in Pure.
 * 
 * Pure syntax:
 * relation->pivot(~[pivotColumns], ~[aggName : x | $x.col : y | $y->plus()])
 * 
 * DuckDB SQL:
 * PIVOT (subquery) ON pivotCol USING agg(valueCol) AS aggName
 */
public record PivotExpression(
        PureExpression source,
        List<String> pivotColumns,
        List<AggregateSpec> aggregates,
        List<Object> staticValues // For static pivot - optional IN clause values
) implements RelationExpression {

    /**
     * Represents an aggregate specification in pivot.
     * Maps to: ~aggName : x | $x.col : y | $y->aggFunc()
     * 
     * For computed expressions (like |1 for count, or $x.a * $x.b), valueColumn is
     * null
     * and valueExpression contains the expression string.
     */
    public record AggregateSpec(
            String name, // Output column name
            String valueColumn, // Column to aggregate (null for expressions)
            String valueExpression, // Expression string for computed values (null for columns)
            String aggFunction // Aggregate function (sum, count, etc.)
    ) {
        public AggregateSpec {
            Objects.requireNonNull(name, "Aggregate name cannot be null");
            Objects.requireNonNull(aggFunction, "Aggregate function cannot be null");
            // Either valueColumn or valueExpression must be non-null
        }

        /**
         * Creates an aggregate spec with a simple column reference.
         */
        public static AggregateSpec column(String name, String column, String aggFunction) {
            return new AggregateSpec(name, column, null, aggFunction);
        }

        /**
         * Creates an aggregate spec with a computed expression.
         */
        public static AggregateSpec expression(String name, String expression, String aggFunction) {
            return new AggregateSpec(name, null, expression, aggFunction);
        }

        /**
         * Returns true if this uses a column reference (vs a computed expression).
         */
        public boolean isColumnBased() {
            return valueColumn != null;
        }
    }

    public PivotExpression {
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
     * Creates a dynamic pivot (no static values).
     */
    public static PivotExpression dynamic(PureExpression source, List<String> pivotColumns,
            List<AggregateSpec> aggregates) {
        return new PivotExpression(source, pivotColumns, aggregates, List.of());
    }

    /**
     * Creates a static pivot with explicit values.
     */
    public static PivotExpression withValues(PureExpression source, List<String> pivotColumns,
            List<Object> staticValues, List<AggregateSpec> aggregates) {
        return new PivotExpression(source, pivotColumns, aggregates, staticValues);
    }

    /**
     * Returns true if this is a static pivot with explicit values.
     */
    public boolean isStatic() {
        return staticValues != null && !staticValues.isEmpty();
    }
}
