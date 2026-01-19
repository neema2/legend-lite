package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Add a calculated column (including window functions) to a Relation.
 * 
 * Syntax variants:
 * 1. Simple expression: extend(~newCol : x | $x.col1 + $x.col2)
 * 2. Window function: extend(~rowNum : row_number()->over(~department))
 * 3. Aggregate window: extend(~runningSum : $x.salary->sum()->over(~department,
 * ~salary))
 * 
 * Examples:
 * 
 * <pre>
 * #>{store::DB.T_PERSON}
 *     ->select(~firstName, ~lastName)
 *     ->extend(~fullName : x | $x.firstName + ' ' + $x.lastName)
 * 
 * Person.all()
 *     ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
 *     ->extend(~rank : rank()->over(~department, ~salary->desc()))
 * </pre>
 * 
 * @param source        The source Relation
 * @param newColumnName The name of the new column
 * @param expression    The lambda expression for calculating the column value
 *                      (may be null for window functions)
 * @param windowSpec    The window function specification (null for simple
 *                      expressions)
 */
public record RelationExtendExpression(
        RelationExpression source,
        String newColumnName,
        LambdaExpression expression,
        WindowFunctionSpec windowSpec) implements RelationExpression {

    /**
     * Window function specification.
     * 
     * @param functionName     The window function (row_number, rank, sum, etc.)
     * @param aggregateColumn  Column for aggregate functions (null for ranking
     *                         functions)
     * @param partitionColumns PARTITION BY columns
     * @param orderColumns     ORDER BY columns with direction
     */
    public record WindowFunctionSpec(
            String functionName,
            String aggregateColumn,
            List<String> partitionColumns,
            List<SortSpec> orderColumns) {

        public WindowFunctionSpec {
            Objects.requireNonNull(functionName, "Function name cannot be null");
            Objects.requireNonNull(partitionColumns, "Partition columns cannot be null");
            Objects.requireNonNull(orderColumns, "Order columns cannot be null");
        }

        /**
         * Creates a ranking window function spec (no aggregate column).
         */
        public static WindowFunctionSpec ranking(String functionName,
                List<String> partitionColumns, List<SortSpec> orderColumns) {
            return new WindowFunctionSpec(functionName, null, partitionColumns, orderColumns);
        }

        /**
         * Creates an aggregate window function spec.
         */
        public static WindowFunctionSpec aggregate(String functionName, String aggregateColumn,
                List<String> partitionColumns, List<SortSpec> orderColumns) {
            return new WindowFunctionSpec(functionName, aggregateColumn, partitionColumns, orderColumns);
        }
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

    /**
     * Creates a simple extend expression (no window function).
     */
    public RelationExtendExpression(RelationExpression source, String newColumnName, LambdaExpression expression) {
        this(source, newColumnName, expression, null);
    }

    /**
     * Creates a window function extend expression.
     */
    public static RelationExtendExpression window(RelationExpression source, String newColumnName,
            WindowFunctionSpec windowSpec) {
        return new RelationExtendExpression(source, newColumnName, null, windowSpec);
    }

    public RelationExtendExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(newColumnName, "Column name cannot be null");
        // Either expression or windowSpec must be non-null
        if (expression == null && windowSpec == null) {
            throw new IllegalArgumentException("Either expression or windowSpec must be provided");
        }
    }

    /**
     * Returns true if this is a window function.
     */
    public boolean isWindowFunction() {
        return windowSpec != null;
    }
}
