package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Add a calculated column (including window functions) to a Relation.
 * 
 * Syntax variants:
 * 1. Simple expression: extend(~newCol : x | $x.col1 + $x.col2)
 * 2. Window function: extend(~rowNum : row_number()->over(~department))
 * 3. Aggregate window: extend(~runningSum : sum(~salary)->over(~department,
 * ~salary))
 * 4. With frame: extend(~runningSum : sum(~salary)->over(~department, ~salary,
 * rows(unbounded(), 0)))
 * 
 * @param source        The source Relation
 * @param newColumnName The name of the new column
 * @param expression    The lambda expression for calculating the column value
 *                      (may be null for window functions)
 * @param windowSpec    The window function specification (null for simple
 *                      expressions)
 */
public record RelationExtendExpression(
        PureExpression source,
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
     * @param frame            Optional frame specification (rows/range)
     */
    public record WindowFunctionSpec(
            String functionName,
            String aggregateColumn,
            List<String> partitionColumns,
            List<SortSpec> orderColumns,
            FrameSpec frame) {

        public WindowFunctionSpec {
            Objects.requireNonNull(functionName, "Function name cannot be null");
            Objects.requireNonNull(partitionColumns, "Partition columns cannot be null");
            Objects.requireNonNull(orderColumns, "Order columns cannot be null");
            // frame can be null (uses SQL default)
        }

        /**
         * Creates a ranking window function spec (no aggregate column, no frame).
         */
        public static WindowFunctionSpec ranking(String functionName,
                List<String> partitionColumns, List<SortSpec> orderColumns) {
            return new WindowFunctionSpec(functionName, null, partitionColumns, orderColumns, null);
        }

        /**
         * Creates a ranking window function spec with frame.
         */
        public static WindowFunctionSpec ranking(String functionName,
                List<String> partitionColumns, List<SortSpec> orderColumns, FrameSpec frame) {
            return new WindowFunctionSpec(functionName, null, partitionColumns, orderColumns, frame);
        }

        /**
         * Creates an aggregate window function spec (no frame).
         */
        public static WindowFunctionSpec aggregate(String functionName, String aggregateColumn,
                List<String> partitionColumns, List<SortSpec> orderColumns) {
            return new WindowFunctionSpec(functionName, aggregateColumn, partitionColumns, orderColumns, null);
        }

        /**
         * Creates an aggregate window function spec with frame.
         */
        public static WindowFunctionSpec aggregate(String functionName, String aggregateColumn,
                List<String> partitionColumns, List<SortSpec> orderColumns, FrameSpec frame) {
            return new WindowFunctionSpec(functionName, aggregateColumn, partitionColumns, orderColumns, frame);
        }

        /**
         * Returns true if this has a frame specification.
         */
        public boolean hasFrame() {
            return frame != null;
        }
    }

    /**
     * Frame specification for window functions (ROWS or RANGE).
     * 
     * Legend-Engine syntax:
     * - rows(start, end)
     * - range(start, end)
     */
    public record FrameSpec(FrameType type, FrameBound start, FrameBound end) {
        public FrameSpec {
            Objects.requireNonNull(type, "Frame type cannot be null");
            Objects.requireNonNull(start, "Frame start cannot be null");
            Objects.requireNonNull(end, "Frame end cannot be null");
        }

        public static FrameSpec rows(FrameBound start, FrameBound end) {
            return new FrameSpec(FrameType.ROWS, start, end);
        }

        public static FrameSpec range(FrameBound start, FrameBound end) {
            return new FrameSpec(FrameType.RANGE, start, end);
        }
    }

    /**
     * Frame type.
     */
    public enum FrameType {
        ROWS, RANGE
    }

    /**
     * Frame boundary.
     * 
     * Legend-Engine encoding:
     * - unbounded() = UNBOUNDED
     * - 0 = CURRENT ROW
     * - negative = N PRECEDING
     * - positive = N FOLLOWING
     */
    public record FrameBound(BoundType type, int offset) {

        public static FrameBound unbounded() {
            return new FrameBound(BoundType.UNBOUNDED, 0);
        }

        public static FrameBound currentRow() {
            return new FrameBound(BoundType.CURRENT_ROW, 0);
        }

        public static FrameBound preceding(int n) {
            return new FrameBound(BoundType.PRECEDING, n);
        }

        public static FrameBound following(int n) {
            return new FrameBound(BoundType.FOLLOWING, n);
        }

        /**
         * Creates from integer value (Legend-Engine encoding).
         */
        public static FrameBound fromInteger(int value) {
            if (value == 0) {
                return currentRow();
            } else if (value < 0) {
                return preceding(-value);
            } else {
                return following(value);
            }
        }
    }

    /**
     * Frame bound type.
     */
    public enum BoundType {
        UNBOUNDED, CURRENT_ROW, PRECEDING, FOLLOWING
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
    public RelationExtendExpression(PureExpression source, String newColumnName, LambdaExpression expression) {
        this(source, newColumnName, expression, null);
    }

    /**
     * Creates a window function extend expression.
     */
    public static RelationExtendExpression window(PureExpression source, String newColumnName,
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
