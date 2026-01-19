package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a window function in the logical plan.
 * 
 * Maps to SQL window function syntax:
 * FUNCTION(column) OVER (PARTITION BY ... ORDER BY ...)
 * 
 * Example Pure:
 * ->extend(~rank : rank()->over(~department, ~salary->desc()))
 * 
 * SQL output:
 * RANK() OVER (PARTITION BY "department" ORDER BY "salary" DESC) AS "rank"
 */
public record WindowExpression(
        WindowFunction function,
        String aggregateColumn,
        java.util.List<String> partitionBy,
        java.util.List<SortSpec> orderBy) {

    /**
     * Window function types.
     */
    public enum WindowFunction {
        // Ranking functions
        ROW_NUMBER,
        RANK,
        DENSE_RANK,
        NTILE,

        // Value functions
        LAG,
        LEAD,
        FIRST_VALUE,
        LAST_VALUE,

        // Aggregate functions (can be used as window functions)
        SUM,
        AVG,
        MIN,
        MAX,
        COUNT
    }

    /**
     * Sort specification with direction.
     */
    public record SortSpec(String column, SortDirection direction) {
        public SortSpec {
            Objects.requireNonNull(column, "Column cannot be null");
            Objects.requireNonNull(direction, "Direction cannot be null");
        }
    }

    /**
     * Sort direction.
     */
    public enum SortDirection {
        ASC, DESC
    }

    public WindowExpression {
        Objects.requireNonNull(function, "Window function cannot be null");
        Objects.requireNonNull(partitionBy, "Partition columns cannot be null");
        Objects.requireNonNull(orderBy, "Order columns cannot be null");
        // aggregateColumn can be null for ranking functions
    }

    /**
     * Creates a ranking window function (no aggregate column).
     */
    public static WindowExpression ranking(
            WindowFunction function,
            java.util.List<String> partitionBy,
            java.util.List<SortSpec> orderBy) {
        return new WindowExpression(function, null, partitionBy, orderBy);
    }

    /**
     * Creates an aggregate window function.
     */
    public static WindowExpression aggregate(
            WindowFunction function,
            String aggregateColumn,
            java.util.List<String> partitionBy,
            java.util.List<SortSpec> orderBy) {
        return new WindowExpression(function, aggregateColumn, partitionBy, orderBy);
    }

    /**
     * Returns true if this is a ranking function (ROW_NUMBER, RANK, etc.)
     */
    public boolean isRankingFunction() {
        return switch (function) {
            case ROW_NUMBER, RANK, DENSE_RANK, NTILE -> true;
            default -> false;
        };
    }

    /**
     * Returns true if this is an aggregate function used as a window function.
     */
    public boolean isAggregateFunction() {
        return switch (function) {
            case SUM, AVG, MIN, MAX, COUNT -> true;
            default -> false;
        };
    }
}
