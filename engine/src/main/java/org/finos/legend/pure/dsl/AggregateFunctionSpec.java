package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Specification for aggregate window functions.
 * 
 * Aggregate functions compute an aggregate value over the window frame.
 * They require a column to aggregate over.
 * 
 * Examples:
 * - sum(~salary)->over(~department, ~date->asc(), rows(unbounded(), 0))
 * - avg(~salary)->over(~department)
 */
public record AggregateFunctionSpec(
        AggregateFunction function,
        String column, // Column to aggregate
        List<String> partitionBy,
        List<WindowFunctionSpec.WindowSortSpec> orderBy,
        WindowFunctionSpec.WindowFrameSpec frame) implements WindowFunctionSpec {

    /**
     * Aggregate function types that can be used as window functions.
     */
    public enum AggregateFunction {
        SUM,
        AVG,
        COUNT,
        MIN,
        MAX,
        STDDEV,
        STDDEV_SAMP,
        STDDEV_POP,
        VARIANCE,
        VAR_SAMP,
        VAR_POP,
        MEDIAN;

        /**
         * Parses a Pure function name to AggregateFunction.
         */
        public static AggregateFunction fromName(String name) {
            return switch (name.toLowerCase()) {
                case "sum" -> SUM;
                case "avg", "average" -> AVG;
                case "count", "size" -> COUNT;
                case "min" -> MIN;
                case "max" -> MAX;
                case "stddev" -> STDDEV;
                case "stddevsample", "stddev_samp" -> STDDEV_SAMP;
                case "stddevpopulation", "stddev_pop" -> STDDEV_POP;
                case "variance" -> VARIANCE;
                case "variancesample", "var_samp" -> VAR_SAMP;
                case "variancepopulation", "var_pop" -> VAR_POP;
                case "median" -> MEDIAN;
                default -> null;
            };
        }

        /**
         * Returns the SQL function name.
         */
        public String toSql() {
            return name();
        }
    }

    /**
     * Creates an aggregate window function spec.
     */
    public static AggregateFunctionSpec of(AggregateFunction function, String column,
            List<String> partitionBy, List<WindowFunctionSpec.WindowSortSpec> orderBy,
            WindowFunctionSpec.WindowFrameSpec frame) {
        return new AggregateFunctionSpec(function, column, partitionBy, orderBy, frame);
    }

    /**
     * Creates an aggregate window function spec without frame (uses SQL default).
     */
    public static AggregateFunctionSpec of(AggregateFunction function, String column,
            List<String> partitionBy, List<WindowFunctionSpec.WindowSortSpec> orderBy) {
        return new AggregateFunctionSpec(function, column, partitionBy, orderBy, null);
    }
}
