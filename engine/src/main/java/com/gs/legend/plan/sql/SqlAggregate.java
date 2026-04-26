package com.gs.legend.plan.sql;

import com.gs.legend.sqlgen.SqlExpr;

import java.util.List;

/**
 * Sealed typed hierarchy for SQL aggregate-shaped functions emitted in
 * {@code GROUP BY ... agg} and {@code FN(...) OVER (...)} positions.
 *
 * <p>Three orthogonal sub-categories with different position constraints:
 * <ul>
 *   <li>{@link Reducer} — folds a column to a scalar. Valid in <b>both</b>
 *       agg context ({@code SUM(x)}) and window context
 *       ({@code SUM(x) OVER (...)}). {@link com.gs.legend.plan.sql.SqlRelation.Agg#fn()}
 *       is typed as {@code Reducer} so {@code Lag} cannot accidentally land
 *       in {@code GROUP BY}.</li>
 *   <li>{@link RankingFn} — ranking functions. Valid <b>only</b> in window
 *       context: {@code ROW_NUMBER()}, {@code RANK()}, {@code DENSE_RANK()},
 *       {@code PERCENT_RANK()}, {@code CUME_DIST()}.</li>
 *   <li>{@link ValueFn} — positional value lookups. Valid <b>only</b> in
 *       window context: {@code FIRST_VALUE}, {@code LAST_VALUE},
 *       {@code LAG}, {@code LEAD}, {@code NTILE}, {@code NTH_VALUE}.</li>
 * </ul>
 *
 * <p>{@link com.gs.legend.sqlgen.SqlExpr.WindowCall#fn()} is typed as
 * {@code SqlAggregate} (any sub-category). Position is encoded by the call
 * site, not by an explicit lift wrapper — {@code Reducer} variants render
 * identically in both positions; the surrounding {@code OVER (...)} clause
 * is the only difference.
 *
 * <p>Each variant carries every operand its SQL form needs as named record
 * fields. There is no separate "extract the arg" helper; rendering
 * pattern-matches on the variant directly. Adding a new aggregate = adding
 * a new {@code record} here + a new arm in
 * {@link com.gs.legend.sqlgen.SQLDialect#render(SqlAggregate)}. javac
 * enforces exhaustiveness via the {@code permits} clauses; no
 * {@code default} arms allowed (AGENTS.md invariant 3).
 *
 * <p>See {@code docs/pipeline-architecture.md} §5.2.
 */
public sealed interface SqlAggregate permits
        SqlAggregate.Reducer,
        SqlAggregate.RankingFn,
        SqlAggregate.ValueFn {

    // =====================================================================
    // Sub-category interfaces
    // =====================================================================

    /**
     * Reducer aggregates: {@code SUM}, {@code COUNT}, {@code AVG},
     * {@code MEDIAN}, {@code PERCENTILE_CONT}, {@code CORR}, etc. Valid in
     * both agg context and window context. {@link com.gs.legend.plan.sql.SqlRelation.Agg#fn()}
     * is typed as {@code Reducer} (not the broader {@link SqlAggregate}) so
     * the type system forbids ranking/value functions in {@code GROUP BY}.
     */
    sealed interface Reducer extends SqlAggregate permits
            Sum, Count, CountStar, Max, Min, Avg, StdDev, Variance, Product,
            Median, Mode, HashCode,
            StdDevPopulation, StdDevSample, VariancePopulation, VarianceSample,
            MaxBy, MinBy, WeightedAvg,
            JoinStrings, PercentileCont, PercentileDisc,
            Corr, CovarPopulation, CovarSample {}

    /**
     * Window-only ranking functions. Zero-operand; meaningless without
     * {@code OVER (...)}. The Pure natives that resolve here all carry
     * window-specific signatures (e.g. {@code Pure.RANK__RELATION_1__WINDOW_1__T_1}).
     */
    sealed interface RankingFn extends SqlAggregate permits
            RowNumber, Rank, DenseRank, PercentRank, CumulativeDistribution {}

    /**
     * Window-only positional value functions. Read a value from a specific
     * row in the frame. Meaningless outside {@code OVER (...)}.
     */
    sealed interface ValueFn extends SqlAggregate permits
            FirstValue, LastValue, Lag, Lead, Ntile, NthValue {}

    // =====================================================================
    // Reducer records (unary)
    // =====================================================================

    /** {@code SUM(expr)}. Pure: {@code sum}, {@code plus} (in agg position). */
    record Sum(SqlExpr expr) implements Reducer {}

    /** {@code COUNT(expr)}. Pure: {@code count}, {@code size}. */
    record Count(SqlExpr expr) implements Reducer {}

    /**
     * {@code COUNT(*)} — count of rows, no operand. Pure: {@code count} called
     * in window position with no value arg (the checker filters context refs
     * out of {@code funcArgs}, leaving an empty list).
     */
    record CountStar() implements Reducer {}

    /** {@code MAX(expr)}. Pure: {@code max}. */
    record Max(SqlExpr expr) implements Reducer {}

    /** {@code MIN(expr)}. Pure: {@code min}. */
    record Min(SqlExpr expr) implements Reducer {}

    /** {@code AVG(expr)}. Pure: {@code avg}, {@code average}, {@code mean}. */
    record Avg(SqlExpr expr) implements Reducer {}

    /** {@code STDDEV(expr)}. Pure: {@code stdDev}, {@code stdev}, {@code std}. */
    record StdDev(SqlExpr expr) implements Reducer {}

    /** {@code VARIANCE(expr)}. Pure: {@code variance}. */
    record Variance(SqlExpr expr) implements Reducer {}

    /** {@code PRODUCT(expr)} (DuckDB-style). Pure: {@code times}. */
    record Product(SqlExpr expr) implements Reducer {}

    /** {@code MEDIAN(expr)}. Pure: {@code median}. */
    record Median(SqlExpr expr) implements Reducer {}

    /** {@code MODE(expr)}. Pure: {@code mode}. */
    record Mode(SqlExpr expr) implements Reducer {}

    /** {@code HASH(expr)} (DuckDB) / dialect-specific hash. Pure: {@code hashCode}. */
    record HashCode(SqlExpr expr) implements Reducer {}

    /** {@code STDDEV_POP(expr)}. Pure: {@code stdDevPopulation}. */
    record StdDevPopulation(SqlExpr expr) implements Reducer {}

    /** {@code STDDEV_SAMP(expr)}. Pure: {@code stdDevSample}. */
    record StdDevSample(SqlExpr expr) implements Reducer {}

    /** {@code VAR_POP(expr)}. Pure: {@code variancePopulation}. */
    record VariancePopulation(SqlExpr expr) implements Reducer {}

    /** {@code VAR_SAMP(expr)}. Pure: {@code varianceSample}. */
    record VarianceSample(SqlExpr expr) implements Reducer {}

    /** {@code ARG_MAX(expr)} (DuckDB) / equivalent. Pure: {@code maxBy}. */
    record MaxBy(SqlExpr expr) implements Reducer {}

    /** {@code ARG_MIN(expr)} (DuckDB) / equivalent. Pure: {@code minBy}. */
    record MinBy(SqlExpr expr) implements Reducer {}

    /** Weighted average. Pure: {@code wavg}. */
    record WeightedAvg(SqlExpr expr) implements Reducer {}

    // =====================================================================
    // Reducer records (multi-operand)
    // =====================================================================

    /**
     * {@code STRING_AGG(expr, separator)}. Pure: {@code joinStrings(values, sep)}.
     * Both operands required — separator carried explicitly so the renderer
     * never falls back to the dialect default.
     */
    record JoinStrings(SqlExpr expr, SqlExpr separator) implements Reducer {}

    /**
     * {@code PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY expr)}. Pure:
     * {@code percentileCont(values, p)}, {@code percentile(values, p, true,
     * true)}. The descending-percentile case
     * ({@code percentile(values, p, false, true)}) is normalised at binding
     * time by inverting {@code p} → {@code 1 - p}; the variant carries only
     * the effective p, so the renderer never sees an "ascending" flag.
     */
    record PercentileCont(SqlExpr expr, SqlExpr p) implements Reducer {}

    /**
     * {@code PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY expr)}. Pure:
     * {@code percentileDisc(values, p)}, {@code percentile(values, p, _,
     * false)}. Same inversion convention as {@link PercentileCont} for
     * descending order.
     */
    record PercentileDisc(SqlExpr expr, SqlExpr p) implements Reducer {}

    /** {@code CORR(x, y)}. Pure: {@code corr(x, y)}. */
    record Corr(SqlExpr x, SqlExpr y) implements Reducer {}

    /** {@code COVAR_POP(x, y)}. Pure: {@code covarPopulation(x, y)}. */
    record CovarPopulation(SqlExpr x, SqlExpr y) implements Reducer {}

    /** {@code COVAR_SAMP(x, y)}. Pure: {@code covarSample(x, y)}. */
    record CovarSample(SqlExpr x, SqlExpr y) implements Reducer {}

    // =====================================================================
    // RankingFn records (zero-operand, window-only)
    // =====================================================================

    /** {@code ROW_NUMBER()}. Pure: {@code rowNumber}. */
    record RowNumber() implements RankingFn {}

    /** {@code RANK()}. Pure: {@code rank}. */
    record Rank() implements RankingFn {}

    /** {@code DENSE_RANK()}. Pure: {@code denseRank}. */
    record DenseRank() implements RankingFn {}

    /** {@code PERCENT_RANK()}. Pure: {@code percentRank}. */
    record PercentRank() implements RankingFn {}

    /** {@code CUME_DIST()}. Pure: {@code cumulativeDistribution}. */
    record CumulativeDistribution() implements RankingFn {}

    // =====================================================================
    // ValueFn records (window-only, positional)
    // =====================================================================

    /** {@code FIRST_VALUE(expr)}. Pure: {@code firstValue}, {@code first} (in window position). */
    record FirstValue(SqlExpr expr) implements ValueFn {}

    /** {@code LAST_VALUE(expr)}. Pure: {@code lastValue}, {@code last} (in window position). */
    record LastValue(SqlExpr expr) implements ValueFn {}

    /** {@code LAG(expr [, offset [, default]])}. Pure: {@code lag}.
     * Args list carries 1–3 values per Pure-source overload. */
    record Lag(List<SqlExpr> args) implements ValueFn {}

    /** {@code LEAD(expr [, offset [, default]])}. Pure: {@code lead}.
     * Args list carries 1–3 values per Pure-source overload. */
    record Lead(List<SqlExpr> args) implements ValueFn {}

    /** {@code NTILE(buckets)}. Pure: {@code ntile}. */
    record Ntile(SqlExpr buckets) implements ValueFn {}

    /** {@code NTH_VALUE(expr, n)}. Pure: {@code nthValue}, {@code nth} (in window position). */
    record NthValue(SqlExpr expr, SqlExpr n) implements ValueFn {}
}
