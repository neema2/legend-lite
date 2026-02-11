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
        String secondColumn, // Second column for bi-variate window functions (corr, covar, wavg, etc.)
        List<String> partitionBy,
        List<SortSpec> orderBy,
        FrameSpec frame,
        Integer offset,
        PostProcessor postProcessor,
        Double percentileValue) { // Optional percentile value for QUANTILE_CONT/QUANTILE_DISC

    /**
     * Post-processor function applied to the window result.
     * E.g., for cumulativeDistribution(...)->round(2), stores function="round",
     * args=[2]
     */
    public record PostProcessor(String function, List<Object> arguments) {
    }

    /**
     * Window function types.
     */
    public enum WindowFunction {
        // Ranking functions
        ROW_NUMBER,
        RANK,
        DENSE_RANK,
        NTILE,
        PERCENT_RANK,
        CUME_DIST,

        // Value functions
        LAG,
        LEAD,
        FIRST_VALUE,
        LAST_VALUE,
        NTH_VALUE,

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
        COVAR_POP,

        // Percentile functions (ordered-set aggregates)
        QUANTILE_CONT,
        QUANTILE_DISC,

        // Mode
        MODE,

        // String aggregation
        STRING_AGG
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

            // Validate that lower bound is not greater than upper bound
            // Frame bounds order: UNBOUNDED PRECEDING < N PRECEDING < CURRENT ROW < N
            // FOLLOWING < UNBOUNDED FOLLOWING
            if (!isValidFrameOrder(start, end)) {
                throw new IllegalArgumentException(
                        "Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!");
            }
        }

        /**
         * Validates that start bound is not after end bound.
         * Order: UNBOUNDED PRECEDING < N PRECEDING < CURRENT ROW < N FOLLOWING <
         * UNBOUNDED FOLLOWING
         * 
         * @param isStartBound true if computing ordinal for start bound, false for end
         *                     bound
         */
        private static boolean isValidFrameOrder(FrameBound start, FrameBound end) {
            double startOrdinal = boundOrdinal(start, true);
            double endOrdinal = boundOrdinal(end, false);
            return startOrdinal <= endOrdinal;
        }

        /**
         * Returns an ordinal value for frame bound ordering.
         * Lower values come before higher values in the frame.
         * 
         * @param isStartBound true if this is the start bound (UNBOUNDED = PRECEDING),
         *                     false if this is the end bound (UNBOUNDED = FOLLOWING)
         */
        private static double boundOrdinal(FrameBound bound, boolean isStartBound) {
            return switch (bound.type()) {
                // UNBOUNDED at start means PRECEDING (min value), at end means FOLLOWING (max
                // value)
                case UNBOUNDED -> isStartBound ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                case PRECEDING -> -bound.offset(); // e.g., 5 PRECEDING = -5
                case CURRENT_ROW -> 0;
                case FOLLOWING -> bound.offset(); // e.g., 5 FOLLOWING = 5
            };
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
     * Uses double for offset to support decimal RANGE bounds (e.g.,
     * 0.5D->_range(2.5))
     */
    public record FrameBound(BoundType type, double offset) {

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
         * Creates a PRECEDING boundary with decimal offset (for RANGE frames).
         */
        public static FrameBound preceding(double n) {
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
         * Creates a FOLLOWING boundary with decimal offset (for RANGE frames).
         */
        public static FrameBound following(double n) {
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

        /**
         * Creates a bound from a double value using Legend-Engine encoding:
         * - 0 = CURRENT ROW
         * - negative = PRECEDING
         * - positive = FOLLOWING
         */
        public static FrameBound fromDouble(double value) {
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
        // percentileValue can be null for non-percentile functions
    }

    /**
     * Creates a ranking window function (no aggregate column, no frame).
     */
    public static WindowExpression ranking(
            WindowFunction function,
            List<String> partitionBy,
            List<SortSpec> orderBy) {
        return new WindowExpression(function, null, null, partitionBy, orderBy, null, null, null, null);
    }

    /**
     * Creates a ranking window function with frame.
     */
    public static WindowExpression ranking(
            WindowFunction function,
            List<String> partitionBy,
            List<SortSpec> orderBy,
            FrameSpec frame) {
        return new WindowExpression(function, null, null, partitionBy, orderBy, frame, null, null, null);
    }

    /**
     * Creates an aggregate window function (no frame).
     */
    public static WindowExpression aggregate(
            WindowFunction function,
            String aggregateColumn,
            List<String> partitionBy,
            List<SortSpec> orderBy) {
        return new WindowExpression(function, aggregateColumn, null, partitionBy, orderBy, null, null, null, null);
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
        return new WindowExpression(function, aggregateColumn, null, partitionBy, orderBy, frame, null, null, null);
    }

    /**
     * Creates a bi-variate aggregate window function (for rowMapper pattern).
     */
    public static WindowExpression bivariate(
            WindowFunction function,
            String aggregateColumn,
            String secondColumn,
            List<String> partitionBy,
            List<SortSpec> orderBy) {
        return new WindowExpression(function, aggregateColumn, secondColumn, partitionBy, orderBy, null, null, null, null);
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
        return new WindowExpression(function, column, null, partitionBy, orderBy, null, offset, null, null);
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
        return new WindowExpression(function, column, null, partitionBy, orderBy, frame, offset, null, null);
    }

    /**
     * Creates a NTILE window function with bucket count.
     */
    public static WindowExpression ntile(
            int bucketCount,
            List<String> partitionBy,
            List<SortSpec> orderBy) {
        return new WindowExpression(WindowFunction.NTILE, null, null, partitionBy, orderBy, null, bucketCount, null, null);
    }

    /**
     * Creates a percentile window function.
     */
    public static WindowExpression percentile(
            WindowFunction function,
            String aggregateColumn,
            double percentileValue,
            List<String> partitionBy,
            List<SortSpec> orderBy) {
        return new WindowExpression(function, aggregateColumn, null, partitionBy, orderBy, null, null, null, percentileValue);
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
            case ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST -> true;
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
