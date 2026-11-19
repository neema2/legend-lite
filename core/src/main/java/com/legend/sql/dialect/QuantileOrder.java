package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlType;
import java.util.List;

/**
 * DuckDB's quantile family takes NO within-group order, so the
 * percentile reducer's order key (the semantic {@code PERCENTILE_x(p)
 * WITHIN GROUP (ORDER BY v [DESC])}) is spelled here, as a named MIR
 * pass: an ASCENDING key drops ({@code quantile_cont(v, p)}); a
 * DESCENDING continuous percentile interpolates over the NEGATED values
 * and negates back (the same interpolation direction as the standard's
 * DESC form — engine golden 1.4 over [1,1.5,2]); a DISCRETE percentile,
 * either way, is the ceil(p*N)-th element of the collected list sorted
 * in its order, the first at p = 0, p a DOUBLE (PERCENTILE_DISC picks the
 * first value whose cume_dist >= p, and Postgres multiplies in float8:
 * ceil(0.28 * 25) is 8 there, 7 in DuckDB's exact quantile_disc). A
 * window-positioned percentile windows every reducer of its encoding with
 * the same window spec.
 */
final class QuantileOrder extends SqlRewriter {
    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlAgg.Reducer r && ordered(r)) {
            return encode(r);
        }
        if (e instanceof SqlExpr.WindowCall w
                && w.fn() instanceof SqlAgg.Reducer r && ordered(r)) {
            return windowize(encode(r), w);
        }
        return e;
    }

    /** An ASCENDING percentile reaches the dialect with NO order key at
     * all (the standard's default), so the empty-order form is in scope
     * too — it is exactly the form the continuous arm must re-spell. */
    private static boolean ordered(SqlAgg.Reducer r) {
        return (r.fn() == SqlAgg.Fn.QUANTILE_CONT
                    || r.fn() == SqlAgg.Fn.QUANTILE_DISC)
                && r.orderBy().size() <= 1 && r.args().size() == 2;
    }

    private static SqlExpr encode(SqlAgg.Reducer r) {
        SqlExpr value = r.args().get(0);
        SqlExpr p = r.args().get(1);
        boolean ascending = r.orderBy().isEmpty()
                || r.orderBy().get(0).ascending();
        if (r.fn() == SqlAgg.Fn.QUANTILE_DISC) {
            return discrete(value, p, r.distinct(), ascending);
        }
        if (ascending) {
            return interpolate(value, p, r.distinct());
        }
        return SqlExpr.Call.of(SqlFn.NEGATE,
                interpolate(SqlExpr.Call.of(SqlFn.NEGATE, value), p,
                        r.distinct()));
    }

    /**
     * The DISCRETE percentile as Postgres's percentile_disc computes it:
     * element max(ceil(p * N), 1) of the non-NULL values sorted in the
     * order (NULLs sort last, and N counts the others), p a DOUBLE. At
     * p = 0, ceil(p*N) is 0, before the 1-based list's first element: the
     * first value is the answer there (pure's percentile too).
     */
    private static SqlExpr discrete(SqlExpr value, SqlExpr p, boolean distinct,
            boolean ascending) {
        return SqlExpr.Call.of(SqlFn.LIST_GET,
                SqlExpr.Call.of(ascending ? SqlFn.LIST_SORT : SqlFn.LIST_SORT_DESC,
                        new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(value),
                                distinct, List.of())),
                SqlExpr.Call.of(SqlFn.GREATEST,
                        new SqlExpr.Cast(
                                SqlExpr.Call.of(SqlFn.CEILING,
                                        SqlExpr.Call.of(SqlFn.TIMES,
                                                new SqlExpr.Cast(p, SqlType.Scalar.DOUBLE, SqlExpr.Reason.DIALECT),
                                                new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                                                        List.of(value), distinct,
                                                        List.of()))),
                                SqlType.Scalar.BIGINT, SqlExpr.Reason.DIALECT),
                        new SqlExpr.IntLit(1)));
    }

    /**
     * The CONTINUOUS percentile, interpolated HERE rather than handed to
     * DuckDB's {@code quantile_cont}.
     *
     * <p>MEASURED 2026-09-09 (the CI leg's one finding): DuckDB's own
     * {@code percentile_cont}/{@code quantile_cont} is NOT
     * architecture-stable. Over the corpus's Firm C quantities
     * (22,27,38,44,45) at p=0.9 it returns 44.600000000000001421 on
     * arm64 and 44.599999999999994316 on x86_64 — one ULP apart, from
     * ONE binary (the v1.4.4 universal build, both slices, same data,
     * reproduced locally under Rosetta). It is not the thread count and
     * not the spelling; it is the compiled interpolation path, an FMA
     * contraction on arm64 that x86-64's baseline does not have. Plain
     * SQL arithmetic is bit-identical on both slices — {@code 44.0 +
     * 0.6*(45.0-44.0)} agrees to the last bit — so spelling the
     * interpolation OURSELVES removes the divergence at its source
     * instead of relocating it into an arch-conditional emission or an
     * arch-conditional roster (both of which just move the conditional
     * and leave two environments disagreeing by design).
     *
     * <p>It is also MORE accurate, not merely more stable: the value
     * both slices now produce is the correctly-rounded double for the
     * engine golden 44.6, which DuckDB's x86-64 build misses.
     *
     * <p>The SQL-standard definition, over the ascending non-null
     * values with {@code idx = p*(n-1)}:
     * {@code lo + (idx - floor(idx)) * (hi - lo)}, where lo and hi are
     * the elements at {@code floor(idx)} and {@code ceil(idx)}.
     * {@code count} counts the non-nulls and {@code list_sort} sorts
     * nulls LAST (verified), so the non-nulls occupy 1..n of the sorted
     * list and {@code list_extract}'s 1-based index is {@code i+1} —
     * the same pair of facts the discrete arm below already rides.
     *
     * <p>Cost, measured on 2M rows over 1,000 groups: 40ms → 125ms for
     * the whole aggregate. Correctness over an aggregate that is rarely
     * a plan's bottleneck.
     */
    private static SqlExpr interpolate(SqlExpr value, SqlExpr p,
            boolean distinct) {
        SqlExpr sorted = SqlExpr.Call.of(SqlFn.LIST_SORT,
                new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(value),
                        distinct, List.of()));
        SqlExpr n = new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(value),
                distinct, List.of());
        // idx = p * (n - 1), the 0-based position in the sorted values
        SqlExpr idx = SqlExpr.Call.of(SqlFn.TIMES, p,
                SqlExpr.Call.of(SqlFn.MINUS, n, new SqlExpr.IntLit(1)));
        SqlExpr floorIdx = SqlExpr.Call.of(SqlFn.FLOOR, idx);
        SqlExpr lo = at(sorted, floorIdx);
        SqlExpr hi = at(sorted, SqlExpr.Call.of(SqlFn.CEILING, idx));
        // lo + (idx - floor(idx)) * (hi - lo)
        return SqlExpr.Call.of(SqlFn.PLUS, lo,
                SqlExpr.Call.of(SqlFn.TIMES,
                        SqlExpr.Call.of(SqlFn.MINUS, idx, floorIdx),
                        SqlExpr.Call.of(SqlFn.MINUS, hi, lo)));
    }

    /** The sorted list's element at a 0-based position (list_extract is
     * 1-based, and the position arrives as a floor/ceil DOUBLE). */
    private static SqlExpr at(SqlExpr sorted, SqlExpr zeroBased) {
        return SqlExpr.Call.of(SqlFn.LIST_GET, sorted,
                SqlExpr.Call.of(SqlFn.PLUS,
                        new SqlExpr.Cast(zeroBased, SqlType.Scalar.BIGINT, SqlExpr.Reason.DIALECT),
                        new SqlExpr.IntLit(1)));
    }

    /** Every reducer inside the encoding takes the window's spec (the
     * encoding's own shapes: reducers under calls and casts). */
    private static SqlExpr windowize(SqlExpr e, SqlExpr.WindowCall w) {
        return switch (e) {
            case SqlAgg.Reducer r -> new SqlExpr.WindowCall(r,
                    w.partitionBy(), w.orderBy(), w.frame());
            case SqlExpr.Call c -> new SqlExpr.Call(c.fn(), c.args().stream()
                    .map(x -> windowize(x, w)).toList());
            case SqlExpr.Cast c -> new SqlExpr.Cast(windowize(c.value(), w),
                    c.target(), c.reason());
            default -> e;
        };
    }
}
