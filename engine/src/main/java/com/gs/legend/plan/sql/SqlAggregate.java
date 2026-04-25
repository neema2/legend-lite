package com.gs.legend.plan.sql;

import com.gs.legend.sqlgen.SqlExpr;

/**
 * Sealed typed hierarchy for SQL aggregates emitted in {@code GROUP BY ... agg}
 * and key-less aggregate-collapse (`SqlRelation.Aggregate`) positions.
 *
 * <p>Replaces the stringly-typed {@code Agg(String function, List<SqlExpr> args)}
 * shape that lived inside {@link SqlRelation.Agg}. Each variant captures
 * exactly the args its SQL form needs (currently all unary except future
 * {@code CountStar}); dispatch in the dialect is exhaustive over the
 * {@code permits} list \u2014 {@code default} arms are forbidden.
 *
 * <p>See {@code docs/pipeline-architecture.md} §5.2.
 */
public sealed interface SqlAggregate permits
        SqlAggregate.Sum,
        SqlAggregate.Count,
        SqlAggregate.Max,
        SqlAggregate.Min,
        SqlAggregate.Avg,
        SqlAggregate.StdDev,
        SqlAggregate.Variance,
        SqlAggregate.Product,
        SqlAggregate.Median,
        SqlAggregate.HashCode,
        SqlAggregate.JoinStrings,
        SqlAggregate.StdDevPopulation,
        SqlAggregate.StdDevSample,
        SqlAggregate.VariancePopulation,
        SqlAggregate.VarianceSample,
        SqlAggregate.PercentileCont,
        SqlAggregate.PercentileDisc,
        SqlAggregate.Corr,
        SqlAggregate.CovarPopulation,
        SqlAggregate.CovarSample,
        SqlAggregate.MaxBy,
        SqlAggregate.MinBy,
        SqlAggregate.WeightedAvg {

    /** {@code SUM(expr)}. Pure: {@code sum}, {@code plus} (in agg position). */
    record Sum(SqlExpr expr) implements SqlAggregate {}

    /** {@code COUNT(expr)}. Pure: {@code count}, {@code size},
     * {@code distinct} (which sets the {@code distinct} flag on {@link SqlRelation.Agg}). */
    record Count(SqlExpr expr) implements SqlAggregate {}

    /** {@code MAX(expr)}. Pure: {@code max}. */
    record Max(SqlExpr expr) implements SqlAggregate {}

    /** {@code MIN(expr)}. Pure: {@code min}. */
    record Min(SqlExpr expr) implements SqlAggregate {}

    /** {@code AVG(expr)}. Pure: {@code avg}, {@code average}, {@code mean}. */
    record Avg(SqlExpr expr) implements SqlAggregate {}

    /** {@code STDDEV(expr)}. Pure: {@code stdDev}, {@code stdev}, {@code std}. */
    record StdDev(SqlExpr expr) implements SqlAggregate {}

    /** {@code VARIANCE(expr)}. Pure: {@code variance}. */
    record Variance(SqlExpr expr) implements SqlAggregate {}

    /** {@code PRODUCT(expr)} (DuckDB-style). Pure: {@code times}. */
    record Product(SqlExpr expr) implements SqlAggregate {}

    /** {@code MEDIAN(expr)}. Pure: {@code median}. */
    record Median(SqlExpr expr) implements SqlAggregate {}

    /** {@code HASH(expr)} (DuckDB) / dialect-specific hash. Pure: {@code hashCode}. */
    record HashCode(SqlExpr expr) implements SqlAggregate {}

    /** {@code STRING_AGG(expr)} / equivalent. Pure: {@code joinStrings}. */
    record JoinStrings(SqlExpr expr) implements SqlAggregate {}

    /** {@code STDDEV_POP(expr)}. Pure: {@code stdDevPopulation}. */
    record StdDevPopulation(SqlExpr expr) implements SqlAggregate {}

    /** {@code STDDEV_SAMP(expr)}. Pure: {@code stdDevSample}. */
    record StdDevSample(SqlExpr expr) implements SqlAggregate {}

    /** {@code VAR_POP(expr)}. Pure: {@code variancePopulation}. */
    record VariancePopulation(SqlExpr expr) implements SqlAggregate {}

    /** {@code VAR_SAMP(expr)}. Pure: {@code varianceSample}. */
    record VarianceSample(SqlExpr expr) implements SqlAggregate {}

    /** {@code QUANTILE_CONT(expr)} (DuckDB) / {@code PERCENTILE_CONT(expr)}.
     * Pure: {@code percentileCont}, {@code percentile} (normalized at lowering). */
    record PercentileCont(SqlExpr expr) implements SqlAggregate {}

    /** {@code QUANTILE_DISC(expr)} / {@code PERCENTILE_DISC(expr)}.
     * Pure: {@code percentileDisc}. */
    record PercentileDisc(SqlExpr expr) implements SqlAggregate {}

    /** {@code CORR(expr)}. Pure: {@code corr}. Single-arg legacy shape;
     * the second column lives inside the Pure lambda's body. */
    record Corr(SqlExpr expr) implements SqlAggregate {}

    /** {@code COVAR_POP(expr)}. Pure: {@code covarPopulation}. */
    record CovarPopulation(SqlExpr expr) implements SqlAggregate {}

    /** {@code COVAR_SAMP(expr)}. Pure: {@code covarSample}. */
    record CovarSample(SqlExpr expr) implements SqlAggregate {}

    /** {@code ARG_MAX(expr)} (DuckDB) / equivalent. Pure: {@code maxBy}. */
    record MaxBy(SqlExpr expr) implements SqlAggregate {}

    /** {@code ARG_MIN(expr)} (DuckDB) / equivalent. Pure: {@code minBy}. */
    record MinBy(SqlExpr expr) implements SqlAggregate {}

    /** Weighted average. Pure: {@code wavg}. Dialect emits dialect-specific
     * synthesized SQL (DuckDB has no native, falls back to passthrough name). */
    record WeightedAvg(SqlExpr expr) implements SqlAggregate {}
}
