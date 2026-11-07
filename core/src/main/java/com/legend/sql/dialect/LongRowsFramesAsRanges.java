// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A window whose ROWS frame reaches past a database's limit as a RANGE frame over its rows' numbers (Snowflake's, 1000
 * rows: "window frame size of 250000 rows exceeds limit (1000 rows)", and an offset past it is refused at either end
 * of a frame, "Window frame offset exceeds threshold"; measured 2026-11-07 on the #TS island's lane,
 * docs/TS_DECISIONS_2026_11_07.md item 10). The select's FROM, after its WHERE, is numbered in a select below, by
 * ROW_NUMBER over the window's partition and order; the window is ordered by that number, its bounds kept: ROWS
 * {@code n} PRECEDING is RANGE {@code n} PRECEDING over a row's number, and a RANGE frame over a number has no limit
 * (measured on Snowflake to 2000 rows back). The select below reads the FROM under its own alias, so every
 * reference of the select still reads it (as {@link WindowsComputedBelow}). Only the aggregates the database takes over
 * such a frame are written so; any other, and a select whose rows are not its FROM's (grouped, distinct, joined), is
 * refused by name.
 */
final class LongRowsFramesAsRanges extends SqlRewriter {

    private final String database;
    private final int limit;
    private final Set<SqlAgg.Fn> overNumbers;

    /** Numbers each rewritten select's row numbers across the statement. */
    private final int[] next = {0};

    /**
     * @param database    the database's name, for a refusal
     * @param limit       the most rows a ROWS frame holds there, and the farthest offset
     * @param overNumbers the aggregates written over a RANGE frame of the rows' numbers (taken by the database, or
     *                    written by its dialect from those it takes)
     */
    LongRowsFramesAsRanges(String database, int limit, Set<SqlAgg.Fn> overNumbers) {
        this.database = database;
        this.limit = limit;
        this.overNumbers = overNumbers;
    }

    @Override
    public SqlQuery rewriteRoot(SqlQuery q) {
        next[0] = 0;
        return rewrite(q);
    }

    @Override
    protected SqlQuery select(SqlSelect s) {
        List<SqlExpr.WindowCall> calls = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            collect(p.expr(), calls);
        }
        if (s.qualify() != null) {
            collect(s.qualify(), calls);
        }
        for (SqlSelect.SortKey k : s.orderBy()) {
            collect(k.expr(), calls);
        }
        if (calls.isEmpty()) {
            return s;
        }
        for (SqlExpr.WindowCall w : calls) {
            if (!(w.fn() instanceof SqlAgg.Reducer r) || !overNumbers.contains(r.fn())) {
                throw new DialectCapability(name(w) + " over a ROWS frame of more than " + limit + " rows reached "
                        + database + " (its limit; the aggregates written over the rows' numbers: " + overNumbers
                        + ")");
            }
        }
        if (!s.groupBy().isEmpty() || s.having() != null || s.distinct() || s.from() instanceof SqlSource.Join
                || s.from() instanceof SqlSource.Dual || s.from().alias() == null || s.from().outputs().isEmpty()) {
            throw new DialectCapability("a window of a ROWS frame of more than " + limit + " rows over grouped,"
                    + " distinct or joined rows reached " + database + " (its limit)");
        }
        String alias = s.from().alias();
        String stem = "__rn" + next[0]++;
        List<OutputCol> fromOutputs = s.from().outputs();
        List<SqlSelect.Projection> below = new ArrayList<>();
        for (OutputCol oc : fromOutputs) {
            below.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, oc), oc.name(), oc));
        }
        // one number per partition and order
        Map<List<Object>, SqlExpr> numbers = new LinkedHashMap<>();
        Map<SqlExpr, SqlExpr> replaced = new LinkedHashMap<>();
        for (SqlExpr.WindowCall w : calls) {
            if (replaced.containsKey(w)) {
                continue;
            }
            SqlExpr number = numbers.computeIfAbsent(List.of(w.partitionBy(), w.orderBy()), k -> {
                OutputCol out = new OutputCol(stem + "_" + numbers.size(), SqlType.Scalar.BIGINT, false);
                below.add(new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER,
                        List.of()), w.partitionBy(), w.orderBy(), null), out.name(), out));
                return SqlExpr.Column.of(alias, out);
            });
            SqlExpr.WindowCall.Frame f = java.util.Objects.requireNonNull(w.frame(), "a collected window's frame");
            replaced.put(w, new SqlExpr.WindowCall(w.fn(), w.partitionBy(),
                    List.of(new SqlSelect.SortKey(number, true, null, null)),
                    new SqlExpr.WindowCall.Frame(SqlExpr.WindowCall.Frame.Kind.RANGE, f.from(), f.to())));
        }
        SqlSelect rows = new SqlSelect(below, false, s.from(), s.where(), List.of(), null, null, List.of(), null,
                null, List.of());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            if (p.expr() instanceof SqlExpr.Star st && (st.table() == null || st.table().equals(alias))) {
                // the FROM's own columns, never the numbers
                for (OutputCol oc : fromOutputs) {
                    ps.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, oc), oc.name(), oc));
                }
                continue;
            }
            SqlExpr x = substitute(p.expr(), replaced);
            ps.add(x == p.expr() ? p : new SqlSelect.Projection(x, p.alias(), p.out()));
        }
        List<SqlSelect.SortKey> order = new ArrayList<>();
        for (SqlSelect.SortKey k : s.orderBy()) {
            SqlExpr e = substitute(k.expr(), replaced);
            order.add(e == k.expr() ? k : new SqlSelect.SortKey(e, k.ascending(), k.nullOrder(), k.outputName()));
        }
        return new SqlSelect(ps, false, new SqlSource.Subselect(rows, alias, null), null, List.of(), null,
                s.qualify() == null ? null : substitute(s.qualify(), replaced), order, s.limit(), s.offset(),
                s.outputs());
    }

    /** Each window call of {@code e} whose ROWS frame is past the limit, into {@code out}. */
    private void collect(SqlExpr e, List<SqlExpr.WindowCall> out) {
        if (e instanceof SqlExpr.WindowCall w && past(w.frame())) {
            out.add(w);
            return;
        }
        for (SqlExpr c : e.children()) {
            collect(c, out);
        }
    }

    /** Whether a ROWS frame holds more rows than the limit, or reaches an offset past it. */
    private boolean past(SqlExpr.WindowCall.@com.legend.base.Nullable Frame f) {
        if (f == null || f.kind() != SqlExpr.WindowCall.Frame.Kind.ROWS || f.computed()
                || f.from() instanceof SqlExpr.WindowCall.Frame.Bound.IntervalPreceding
                || f.from() instanceof SqlExpr.WindowCall.Frame.Bound.IntervalFollowing
                || f.to() instanceof SqlExpr.WindowCall.Frame.Bound.IntervalPreceding
                || f.to() instanceof SqlExpr.WindowCall.Frame.Bound.IntervalFollowing) {
            return false;   // a frame of rows by number alone (an interval's frame is RANGE, a computed one checked)
        }
        Long from = offset(f.from());
        Long to = offset(f.to());
        if (from != null && Math.abs(from) > limit || to != null && Math.abs(to) > limit) {
            return true;
        }
        return from != null && to != null && to - from + 1 > limit;
    }

    /** A bound's offset from the current row (preceding negative), or null when unbounded ({@link #past} reads no
     *  interval or computed bound). */
    private static @com.legend.base.Nullable Long offset(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> -p.n().longValue();
            case SqlExpr.WindowCall.Frame.Bound.Following f -> f.n().longValue();
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> 0L;
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> null;
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> null;
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding i -> throw unread(b);
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing i -> throw unread(b);
            case SqlExpr.WindowCall.Frame.Bound.ComputedPreceding c -> throw unread(b);
            case SqlExpr.WindowCall.Frame.Bound.ComputedFollowing c -> throw unread(b);
        };
    }

    private static IllegalStateException unread(SqlExpr.WindowCall.Frame.Bound b) {
        return new IllegalStateException("a frame's offset read where it has none: " + b);
    }

    private static String name(SqlExpr.WindowCall w) {
        return switch (w.fn()) {
            case SqlAgg.Reducer r -> r.fn().toString();
            case SqlAgg.RankingFn r -> r.fn().toString();
            case SqlAgg.ValueFn v -> v.fn().toString();
        };
    }

    private static SqlExpr substitute(SqlExpr e, Map<SqlExpr, SqlExpr> replaced) {
        SqlExpr r = replaced.get(e);
        if (r != null) {
            return r;
        }
        return e.children().isEmpty() ? e : e.mapChildren(c -> substitute(c, replaced));
    }
}
