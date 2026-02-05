package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Sealed interface for all window function specifications.
 * 
 * This provides compile-time type safety for window functions:
 * - RankingFunctionSpec: row_number, rank, dense_rank, percent_rank, cume_dist,
 * ntile
 * - ValueFunctionSpec: lag, lead, first_value, last_value, nth_value
 * - AggregateFunctionSpec: sum, avg, count, min, max, stddev, variance, etc.
 * 
 * Each subtype has exactly the parameters it needs, eliminating stringly-typed
 * code.
 */
public sealed interface WindowFunctionSpec permits
        RankingFunctionSpec, ValueFunctionSpec, AggregateFunctionSpec, PostProcessedWindowFunctionSpec {

    /**
     * Columns to partition the window by.
     */
    List<String> partitionBy();

    /**
     * Order specification within each partition.
     */
    List<WindowSortSpec> orderBy();

    /**
     * Frame specification (null = SQL default).
     */
    WindowFrameSpec frame();

    // ===================== Shared Types =====================

    /**
     * Sort specification for window ORDER BY clause.
     */
    record WindowSortSpec(String column, SortDirection direction) {
        public enum SortDirection {
            ASC, DESC
        }

        public static WindowSortSpec asc(String column) {
            return new WindowSortSpec(column, SortDirection.ASC);
        }

        public static WindowSortSpec desc(String column) {
            return new WindowSortSpec(column, SortDirection.DESC);
        }
    }

    /**
     * Frame specification (ROWS or RANGE).
     */
    record WindowFrameSpec(FrameType type, FrameBound start, FrameBound end) {
        public enum FrameType {
            ROWS, RANGE
        }

        public static WindowFrameSpec rows(FrameBound start, FrameBound end) {
            return new WindowFrameSpec(FrameType.ROWS, start, end);
        }

        public static WindowFrameSpec range(FrameBound start, FrameBound end) {
            return new WindowFrameSpec(FrameType.RANGE, start, end);
        }
    }

    /**
     * Frame boundary.
     */
    record FrameBound(BoundType type, int offset) {
        public enum BoundType {
            UNBOUNDED, CURRENT_ROW, PRECEDING, FOLLOWING
        }

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
    }
}
