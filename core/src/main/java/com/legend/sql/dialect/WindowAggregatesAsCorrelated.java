// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.TypeFact;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A window call of an aggregate the database has no window form of -- MariaDB's GROUP_CONCAT (measured on 11.4, as
 * its JSON_ARRAYAGG: "doesn't yet support 'JSON_ARRAYAGG() aggregate as window function'") -- as the same aggregate
 * over the window's rows, a correlated scalar subquery (2026-10-20, the parity phase: eleven Relation tests on MariaDB
 * and on MySQL, which inherits this dialect). The dialect names the aggregates; a windowed list stays the dialect's
 * own (a window's reduce reads it: written here, its values came back as the JSON carrier's, and the database judge
 * refused what the host judge held, measured on channel B).
 *
 * <p>The select's FROM is read numbered, under its own alias, so every reference of the select still reads it: each
 * row with its position in its partition in the window's order ({@code ROW_NUMBER}, ties broken by every column of
 * the FROM, so both readings of it number the rows alike), its rank there ({@code RANK}: a RANGE frame's peers share
 * it), and the window's partition values and arguments as columns. Each window call is then the aggregate of the
 * numbered rows whose partition values equal the row's ({@code <=>}, NULLs one partition as a window's are) inside
 * the row's frame, ordered by position: a ROWS frame by the positions, a RANGE frame of CURRENT ROW and UNBOUNDED
 * bounds by the ranks. The database computes every value; this pass only moves the shape. A RANGE frame with offsets,
 * an interval frame, an aggregate with its own order, and a select that groups, is distinct or reads a join are left
 * as they are, refused by name where they are written.
 */
final class WindowAggregatesAsCorrelated extends SqlRewriter {

    /** The aggregates rewritten: those the dialect writes only grouped. */
    private final Set<SqlAgg.Fn> unwindowed;

    WindowAggregatesAsCorrelated(Set<SqlAgg.Fn> unwindowed) {
        this.unwindowed = java.util.Collections.unmodifiableSet(unwindowed.isEmpty()
                ? java.util.EnumSet.noneOf(SqlAgg.Fn.class) : java.util.EnumSet.copyOf(unwindowed));
    }

    /** Numbers each rewritten select's helper columns across the statement. */
    private final int[] next = {0};

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
        if (calls.isEmpty() || !s.groupBy().isEmpty() || s.having() != null || s.distinct()
                || s.qualify() != null || s.from() instanceof SqlSource.Join || s.from() instanceof SqlSource.Dual
                || s.from().alias() == null
                || s.from().outputs().isEmpty()) {
            return s;
        }
        for (SqlExpr.WindowCall w : calls) {
            if (!rewritable(w)) {
                return s;
            }
        }
        String alias = s.from().alias();
        String stem = "__wa" + next[0]++;
        List<OutputCol> fromOutputs = s.from().outputs();

        // the FROM numbered: its own columns, then each call's partition values, arguments, position and rank
        List<SqlSelect.Projection> numbered = new ArrayList<>();
        for (OutputCol oc : fromOutputs) {
            numbered.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, oc), oc.name(), oc));
        }
        List<SqlSelect.SortKey> tiebreak = new ArrayList<>();
        for (OutputCol oc : fromOutputs) {
            tiebreak.add(new SqlSelect.SortKey(SqlExpr.Column.of(alias, oc), true, null, null));
        }
        Map<SqlExpr.WindowCall, String> names = new LinkedHashMap<>();
        for (SqlExpr.WindowCall w : calls) {
            if (names.containsKey(w)) {
                continue;
            }
            String c = stem + "c" + names.size();
            names.put(w, c);
            for (int i = 0; i < w.partitionBy().size(); i++) {
                numbered.add(helper(w.partitionBy().get(i), c + "p" + i));
            }
            SqlAgg.Reducer r = (SqlAgg.Reducer) w.fn();
            for (int i = 0; i < r.args().size(); i++) {
                if (!(r.args().get(i) instanceof SqlExpr.StringLit)) {
                    numbered.add(helper(r.args().get(i), c + "a" + i));
                }
            }
            List<SqlSelect.SortKey> order = new ArrayList<>(w.orderBy());
            order.addAll(tiebreak);
            numbered.add(new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER,
                    List.of()), w.partitionBy(), order, null), c + "n",
                    new OutputCol(c + "n", SqlType.Scalar.BIGINT, false)));
            SqlExpr.WindowCall.Frame f = frame(w);
            if (f != null && f.kind() == SqlExpr.WindowCall.Frame.Kind.RANGE) {
                numbered.add(new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.RANK,
                        List.of()), w.partitionBy(), w.orderBy(), null), c + "k",
                        new OutputCol(c + "k", SqlType.Scalar.BIGINT, false)));
            }
        }
        SqlSelect rows = new SqlSelect(numbered, false, s.from(), s.where(), List.of(), null, null, List.of(), null,
                null, List.of());

        // the select over the numbered rows, each call the aggregate of its frame's rows
        String inner = stem + "b";
        Map<SqlExpr, SqlExpr> replaced = new LinkedHashMap<>();
        for (var e : names.entrySet()) {
            replaced.put(e.getKey(), aggregateOfFrame(e.getKey(), e.getValue(), rows, alias, inner));
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            if (p.expr() instanceof SqlExpr.Star st && (st.table() == null || st.table().equals(alias))) {
                // the FROM's own columns, never the numbering's
                for (OutputCol oc : fromOutputs) {
                    ps.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, oc), oc.name(), oc));
                }
                continue;
            }
            SqlExpr x = substitute(p.expr(), replaced);
            ps.add(x == p.expr() ? p : new SqlSelect.Projection(x, p.alias(), p.out()));
        }
        return new SqlSelect(ps, false, new SqlSource.Subselect(rows, alias, null), null, List.of(), null, null,
                s.orderBy(), s.limit(), s.offset(), s.outputs());
    }

    /** Whether {@code w} is a call this pass writes: an unwindowed aggregate without its own order, over a frame of
     *  positions or of CURRENT ROW and UNBOUNDED ranks. */
    private boolean rewritable(SqlExpr.WindowCall w) {
        if (!(w.fn() instanceof SqlAgg.Reducer r) || !unwindowed.contains(r.fn()) || !r.orderBy().isEmpty()) {
            return false;
        }
        SqlExpr.WindowCall.Frame f = w.frame();
        if (f == null) {
            return true;
        }
        if (f.kind() == SqlExpr.WindowCall.Frame.Kind.ROWS) {
            return !(f.from() instanceof SqlExpr.WindowCall.Frame.Bound.IntervalPreceding
                    || f.from() instanceof SqlExpr.WindowCall.Frame.Bound.IntervalFollowing
                    || f.to() instanceof SqlExpr.WindowCall.Frame.Bound.IntervalPreceding
                    || f.to() instanceof SqlExpr.WindowCall.Frame.Bound.IntervalFollowing);
        }
        return rankBound(f.from()) && rankBound(f.to());
    }

    private static boolean rankBound(SqlExpr.WindowCall.Frame.Bound b) {
        return b instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding
                || b instanceof SqlExpr.WindowCall.Frame.Bound.CurrentRow
                || b instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing;
    }

    private void collect(SqlExpr e, List<SqlExpr.WindowCall> out) {
        if (e instanceof SqlExpr.WindowCall w && w.fn() instanceof SqlAgg.Reducer r && unwindowed.contains(r.fn())) {
            out.add(w);
            return;
        }
        for (SqlExpr c : e.children()) {
            collect(c, out);
        }
    }

    private static SqlSelect.Projection helper(SqlExpr value, String name) {
        OutputCol out = value.type() instanceof TypeFact.Typed t ? new OutputCol(name, t.type(), true) : null;
        return new SqlSelect.Projection(value, name, out);
    }

    /** {@code w} as the aggregate of the numbered rows in the outer row's frame: {@code (SELECT agg FROM (rows) inner
     *  WHERE partition values equal AND frame)}. */
    private static SqlExpr aggregateOfFrame(SqlExpr.WindowCall w, String c, SqlSelect rows, String outer,
            String inner) {
        SqlAgg.Reducer r = (SqlAgg.Reducer) w.fn();
        List<SqlExpr> args = new ArrayList<>();
        for (int i = 0; i < r.args().size(); i++) {
            SqlExpr a = r.args().get(i);
            args.add(a instanceof SqlExpr.StringLit ? a : SqlExpr.Column.derived(inner, c + "a" + i));
        }
        // a joined or listed value keeps the window's order; an arg-max is no ordered aggregate
        boolean ordered = r.fn() == SqlAgg.Fn.STRING_AGG || r.fn() == SqlAgg.Fn.LIST;
        SqlAgg.Reducer agg = new SqlAgg.Reducer(r.fn(), args, r.distinct(), ordered ? List.of(new SqlSelect.SortKey(
                SqlExpr.Column.derived(inner, c + "n"), true, null, null)) : List.of());
        List<SqlExpr> conds = new ArrayList<>();
        for (int i = 0; i < w.partitionBy().size(); i++) {
            conds.add(SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, SqlExpr.Column.derived(inner, c + "p" + i),
                    SqlExpr.Column.derived(outer, c + "p" + i)));
        }
        SqlExpr.WindowCall.Frame f = frame(w);
        if (f != null) {
            String col = f.kind() == SqlExpr.WindowCall.Frame.Kind.ROWS ? c + "n" : c + "k";
            SqlExpr mine = SqlExpr.Column.derived(inner, col);
            SqlExpr row = SqlExpr.Column.derived(outer, col);
            SqlExpr lo = bound(f.from(), row);
            SqlExpr hi = bound(f.to(), row);
            if (lo != null) {
                conds.add(SqlExpr.Call.of(SqlFn.GREATER_EQUAL, mine, lo));
            }
            if (hi != null) {
                conds.add(SqlExpr.Call.of(SqlFn.LESS_EQUAL, mine, hi));
            }
        }
        SqlExpr where = null;
        for (SqlExpr cond : conds) {
            where = where == null ? cond : SqlExpr.Call.of(SqlFn.AND, where, cond);
        }
        SqlSelect frame = new SqlSelect(List.of(new SqlSelect.Projection(agg, null, null)), false,
                new SqlSource.Subselect(rows, inner, null), where, List.of(), null, null, List.of(), null, null,
                List.of());
        return new SqlExpr.ScalarSubquery(frame);
    }

    /** {@code w}'s frame: its own, an ordered window's default ({@code RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
     *  ROW}), or null for an unordered window's whole partition. */
    private static SqlExpr.WindowCall.@com.legend.base.Nullable Frame frame(SqlExpr.WindowCall w) {
        if (w.frame() != null || w.orderBy().isEmpty()) {
            return w.frame();
        }
        return new SqlExpr.WindowCall.Frame(SqlExpr.WindowCall.Frame.Kind.RANGE,
                new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding(),
                new SqlExpr.WindowCall.Frame.Bound.CurrentRow());
    }

    /** The position (or rank) a frame bound names relative to the row's, or null for an unbounded one. */
    private static @com.legend.base.Nullable SqlExpr bound(SqlExpr.WindowCall.Frame.Bound b, SqlExpr row) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding ignored -> null;
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing ignored -> null;
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow ignored -> row;
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> SqlExpr.Call.of(SqlFn.MINUS, row,
                    new SqlExpr.IntLit(p.n().longValue()));
            case SqlExpr.WindowCall.Frame.Bound.Following p -> SqlExpr.Call.of(SqlFn.PLUS, row,
                    new SqlExpr.IntLit(p.n().longValue()));
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding ignored ->
                    throw new IllegalStateException("an interval bound passed rewritable()");
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing ignored ->
                    throw new IllegalStateException("an interval bound passed rewritable()");
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
