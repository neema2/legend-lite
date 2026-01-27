package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents a window function in the logical plan.
 * 
 * Maps to SQL window function syntax:
 * FUNCTION(column) OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE BETWEEN ...
 * AND ...)
 * 
 * Example Pure:
 * ->extend(~rank : rank()->over(~department, ~salary->desc()))
 * ->extend(~runningSum : sum(~value)->over(~dept, ~date->asc(),
 * rows(unbounded(), 0)))
 * 
 * SQL output:
 * RANK() OVER (PARTITION BY "department" ORDER BY "salary" DESC) AS "rank"
 * SUM("value") OVER (PARTITION BY "dept" ORDER BY "date" ASC ROWS BETWEEN
 * UNBOUNDED PRECEDING AND CURRENT ROW)
 */
public record WindowExpression(
        WindowFunction function,
        String aggregateColumn,
        List<String> partitionBy,
        List<SortSpec> orderBy,
        FrameSpec frame,
        Integer offset) { // Optional offset for LAG/LEAD

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
        COUNT,

        // Statistical aggregate functions
        STDDEV,
        STDDEV_SAMP,
        STDDEV_POP,
        VARIANCE,
        VAR_SAMP,
        VAR_POP,
        MEDIAN,
        CORR,
        COVAR_SAMP,
        COVAR_POP
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
     * Frame specification for window functions.
     * 
     * Follows Legend-Engine syntax:
     * - rows(start, end) -- ROWS frame
     * - range(start, end) -- RANGE frame
     * 
     * Where bounds are:
     * - unbounded() -- UNBOUNDED
     * - 0 -- CURRENT ROW
     * - negative -- N PRECEDING
     * - positive -- N FOLLOWING
     */
    public record FrameSpec(FrameType type, FrameBound start, FrameBound end) {
        public FrameSpec {
            Objects.requireNonNull(type, "Frame type cannot be null");
            Objects.requireNonNull(start, "Frame start cannot be null");
            Objects.requireNonNull(end, "Frame end cannot be null");
        }

        /**
         * Creates a ROWS frame.
         */
        public static FrameSpec rows(FrameBound start, FrameBound end) {
            return new FrameSpec(FrameType.ROWS, start, end);
        }

        /**
         * Creates a RANGE frame.
         */
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
     * Frame boundary specification.
     */
    public record FrameBound(BoundType type, int offset) {

        /**
         * Creates an UNBOUNDED boundary.
         */
        public static FrameBound unbounded() {
            return new FrameBound(BoundType.UNBOUNDED, 0);
        }

        /**
         * Creates a CURRENT ROW boundary.
         */
        public static FrameBound currentRow() {
            return new FrameBound(BoundType.CURRENT_ROW, 0);
        }

        /**
         * Creates a PRECEDING boundary with offset.
         */
        public static FrameBound preceding(int n) {
            if (n < 0)
                throw new IllegalArgumentException("Preceding offset must be non-negative");
            return new FrameBound(BoundType.PRECEDING, n);
        }

        /**
         * Creates a FOLLOWING boundary with offset.
         */
        public static FrameBound following(int n) {
            if (n < 0)
                throw new IllegalArgumentException("Following offset must be non-negative");
            return new FrameBound(BoundType.FOLLOWING, n);
        }

        /**
         * Creates a bound from an integer value using Legend-Engine encoding:
         * - unbounded represented by null in Pure
         * - 0 = CURRENT ROW
         * - negative = PRECEDING
         * - positive = FOLLOWING
         */
        public static FrameBound fromInteger(int value) {
            if (value == 0) {
                return currentRow();
            } else if (value < 0) {
                return preceding(-value); // Convert negative to positive offset
            } else {
                return following(value);
            }
        }
    }

    /**
     * Frame bound type.
     */
    public enum BoundType {
        UNBOUNDED,
        CURRENT_ROW,
        PRECEDING,
        FOLLOWING
    }

    public WindowExpression {
        Objects.requireNonNull(function, "Window function cannot be null");
        Objects.requireNonNull(partitionBy, "Partition columns cannot be null");
        Objects.requireNonNull(orderBy, "Order columns cannot be null");
        // aggregateColumn can be null for ranking functions
        // frame can be null (uses SQL default)
    }

    /**
     * Creates a ranking window function (no aggregate column, no frame).
     */
    public static WindowExpression ranking(
            WindowFunction function,
            List<String> partitionBy,
            List<SortSpec> orderBy) {
        return new WindowExpression(function, null, partitionBy, orderBy, null, null);
    }

    /**
     * Creates a ranking window function with frame.
     */
    public static WindowExpression ranking(
            WindowFunction function,
            List<String> partitionBy,
            List<SortSpec> orderBy,
            FrameSpec frame) {
        return new WindowExpression(function, null, partitionBy, orderBy, frame, null);
    }

    /**
     * Creates an aggregate window function (no frame).
     */
    public static WindowExpression aggregate(
            WindowFunction function,
            String aggregateColumn,
            List<String> partitionBy,
            List<SortSpec> orderBy) {
        return new WindowExpression(function, aggregateColumn, partitionBy, orderBy, null, null);
    }

    /**
     * Creates an aggregate window function with frame.
     */
    public static WindowExpression aggregate(
            WindowFunction function,
            String aggregateColumn,
            List<String> partitionBy,
            List<SortSpec> orderBy,
            FrameSpec frame) {
        return new WindowExpression(function, aggregateColumn, partitionBy, orderBy, frame, null);
    }

    /**
     * Creates a LAG or LEAD window function with offset.
     */
    public static WindowExpression lagLead(
            WindowFunction function,
            String column,
            int offset,
            List<String> partitionBy,
            List<SortSpec> orderBy) {
        return new WindowExpression(function, column, partitionBy, orderBy, null, offset);
    }

    /**
     * Creates a LAG, LEAD, FIRST_VALUE, or LAST_VALUE window function with optional
     * frame.
     */
    public static WindowExpression lagLead(
            WindowFunction function,
            String column,
            int offset,
            List<String> partitionBy,
            List<SortSpec> orderBy,
            FrameSpec frame) {
        return new WindowExpression(function, column, partitionBy, orderBy, frame, offset);
    }

    /**
     * Creates a NTILE window function with bucket count.
     */
    public static WindowExpression ntile(
            int bucketCount,
            List<String> partitionBy,
            List<SortSpec> orderBy) {
        return new WindowExpression(WindowFunction.NTILE, null, partitionBy, orderBy, null, bucketCount);
    }

    /**
     * Returns true if this window has a frame specification.
     */
    public boolean hasFrame() {
        return frame != null;
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
