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
        String secondColumn, // Second column for bi-variate functions (corr, covar, wavg, etc.)
        List<String> partitionBy,
        List<WindowFunctionSpec.WindowSortSpec> orderBy,
        WindowFunctionSpec.WindowFrameSpec frame,
        Double percentileValue) implements WindowFunctionSpec {

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
        MEDIAN,
        MODE,
        CORR,
        COVAR_SAMP,
        COVAR_POP,
        PERCENTILE_CONT,
        PERCENTILE_DISC,
        STRING_AGG,
        WAVG,
        ARG_MAX,
        ARG_MIN;

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
                case "mode" -> MODE;
                case "corr" -> CORR;
                case "covarsample", "covar_samp" -> COVAR_SAMP;
                case "covarpopulation", "covar_pop" -> COVAR_POP;
                case "percentile", "percentilecont", "percentile_cont" -> PERCENTILE_CONT;
                case "percentiledisc", "percentile_disc" -> PERCENTILE_DISC;
                case "joinstrings", "string_agg" -> STRING_AGG;
                case "wavg" -> WAVG;
                case "maxby" -> ARG_MAX;
                case "minby" -> ARG_MIN;
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
        return new AggregateFunctionSpec(function, column, null, partitionBy, orderBy, frame, null);
    }

    /**
     * Creates an aggregate window function spec without frame (uses SQL default).
     */
    public static AggregateFunctionSpec of(AggregateFunction function, String column,
            List<String> partitionBy, List<WindowFunctionSpec.WindowSortSpec> orderBy) {
        return new AggregateFunctionSpec(function, column, null, partitionBy, orderBy, null, null);
    }

    /**
     * Creates a bi-variate aggregate window function spec (for rowMapper pattern).
     */
    public static AggregateFunctionSpec bivariate(AggregateFunction function, String column, String secondColumn,
            List<String> partitionBy, List<WindowFunctionSpec.WindowSortSpec> orderBy) {
        return new AggregateFunctionSpec(function, column, secondColumn, partitionBy, orderBy, null, null);
    }

    /**
     * Creates a percentile aggregate window function spec.
     */
    public static AggregateFunctionSpec percentile(AggregateFunction function, String column,
            double percentileValue,
            List<String> partitionBy, List<WindowFunctionSpec.WindowSortSpec> orderBy) {
        return new AggregateFunctionSpec(function, column, null, partitionBy, orderBy, null, percentileValue);
    }
}
