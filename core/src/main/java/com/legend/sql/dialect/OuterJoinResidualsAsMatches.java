// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A LEFT join whose condition holds a conjunct that reads both sides and is no equality, for a database whose outer
 * join keeps no such condition with SQL's NULLs for the missing side (ClickHouse 25.8: "contains column from left and
 * right table, which is not supported with join_use_nulls", measured; its INNER join keeps one): the condition's
 * MATCHES first, each distinct tuple of the left columns the condition reads INNER-joined to the right side on it,
 * then the left side LEFT-joined to those matches on that tuple, null-safe. The right side's alias names the matches,
 * so every read of its columns reads them unchanged; a left row's matches depend on that tuple alone, so a left row
 * matches exactly what it matched. A select that projects a star keeps its join (the matches carry the tuple's
 * columns too), refused by name where it is written. The database computes every value; this pass only moves the
 * shape.
 */
final class OuterJoinResidualsAsMatches extends SqlRewriter {

    /** Numbers each rewritten join's helper names across the statement. */
    private final int[] next = {0};

    @Override
    public SqlQuery rewriteRoot(SqlQuery q) {
        next[0] = 0;
        return rewrite(q);
    }

    @Override
    protected SqlQuery select(SqlSelect s) {
        if (!(s.from() instanceof SqlSource.Join)
                || s.projections().stream().anyMatch(p -> p.expr() instanceof SqlExpr.Star
                        || p.expr() instanceof SqlExpr.StarExcept)) {
            return s;
        }
        SqlSource from = joins(s.from());
        return from == s.from() ? s : s.withFrom(from);
    }

    private SqlSource joins(SqlSource src) {
        if (!(src instanceof SqlSource.Join j)) {
            return src;
        }
        SqlSource left = joins(j.left());
        SqlSource right = j.right() instanceof SqlSource.Join ? j.right() : joins(j.right());
        SqlSource.Join join = left == j.left() && right == j.right() ? j
                : new SqlSource.Join(left, right, j.kind(), j.on());
        SqlSource matched = matched(join);
        return matched == null ? join : matched;
    }

    /** {@code j} as its matches, or null when it is no LEFT join with a residual both sides read. */
    private SqlSource.@com.legend.base.Nullable Join matched(SqlSource.Join j) {
        SqlExpr on = j.on();
        String rightAlias = j.right().alias();
        if (j.kind() != SqlSource.Join.Kind.LEFT || on == null || rightAlias == null
                || j.right() instanceof SqlSource.Join || hasSubquery(on)) {
            return null;
        }
        Set<String> lefts = new LinkedHashSet<>();
        aliases(j.left(), lefts);
        boolean residual = false;
        for (SqlExpr c : conjuncts(on)) {
            Set<String> read = new LinkedHashSet<>();
            tablesRead(c, read);
            boolean both = read.contains(rightAlias) && read.stream().anyMatch(lefts::contains);
            if (both && !(c instanceof SqlExpr.Call call && call.fn() == SqlFn.EQUAL)) {
                residual = true;
            }
        }
        if (!residual) {
            return null;
        }
        // the left columns the condition reads, each typed
        Map<SqlExpr.Column, String> refs = new LinkedHashMap<>();
        if (!leftColumns(on, lefts, refs)) {
            return null;
        }
        String stem = "__oj" + next[0]++;
        String keysAlias = stem + "l";
        List<SqlSelect.Projection> keyCols = new ArrayList<>();
        List<SqlSelect.Projection> matchCols = new ArrayList<>();
        Map<SqlExpr, SqlExpr> toKeys = new LinkedHashMap<>();
        List<SqlExpr> rejoin = new ArrayList<>();
        int i = 0;
        for (SqlExpr.Column c : refs.keySet()) {
            com.legend.sql.TypeFact.Typed t = (com.legend.sql.TypeFact.Typed) c.type();
            OutputCol key = new OutputCol(stem + "c" + i, t.type(), true);
            OutputCol match = new OutputCol(stem + "k" + i, t.type(), true);
            keyCols.add(new SqlSelect.Projection(c, key.name(), key));
            SqlExpr.Column keyRead = SqlExpr.Column.of(keysAlias, key);
            toKeys.put(c, keyRead);
            matchCols.add(new SqlSelect.Projection(keyRead, match.name(), match));
            rejoin.add(SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, c, SqlExpr.Column.of(rightAlias, match)));
            i++;
        }
        matchCols.add(new SqlSelect.Projection(new SqlExpr.Star(rightAlias), null, null));
        SqlSelect keys = SqlSelect.starOf(j.left()).withProjections(keyCols).withDistinct();
        SqlSource.Join inner = new SqlSource.Join(new SqlSource.Subselect(keys, keysAlias, null), j.right(),
                SqlSource.Join.Kind.INNER, substitute(on, toKeys));
        SqlSelect matches = SqlSelect.starOf(inner).withProjections(matchCols);
        SqlExpr keyOn = rejoin.get(0);
        for (int k = 1; k < rejoin.size(); k++) {
            keyOn = SqlExpr.Call.of(SqlFn.AND, keyOn, rejoin.get(k));
        }
        return new SqlSource.Join(j.left(), new SqlSource.Subselect(matches, rightAlias, null),
                SqlSource.Join.Kind.LEFT, keyOn);
    }

    /** Each column of a left alias {@code e} reads, into {@code out}; false when one is of no typed slot. */
    private static boolean leftColumns(SqlExpr e, Set<String> lefts, Map<SqlExpr.Column, String> out) {
        if (e instanceof SqlExpr.Column c && c.table() != null && lefts.contains(c.table())) {
            if (!(c.type() instanceof com.legend.sql.TypeFact.Typed)) {
                return false;
            }
            out.putIfAbsent(c, c.name());
            return true;
        }
        for (SqlExpr child : e.children()) {
            if (!leftColumns(child, lefts, out)) {
                return false;
            }
        }
        return true;
    }

    /** The aliases {@code e}'s columns read (the condition holds no subquery: checked first). */
    private static void tablesRead(SqlExpr e, Set<String> out) {
        if (e instanceof SqlExpr.Column c && c.table() != null) {
            out.add(c.table());
        }
        for (SqlExpr child : e.children()) {
            tablesRead(child, out);
        }
    }

    private static boolean hasSubquery(SqlExpr e) {
        return e instanceof SqlExpr.ScalarSubquery || e instanceof SqlExpr.Exists || e instanceof SqlExpr.InSubquery
                || e instanceof SqlExpr.Quantified || e.children().stream().anyMatch(
                        OuterJoinResidualsAsMatches::hasSubquery);
    }

    private static SqlExpr substitute(SqlExpr e, Map<SqlExpr, SqlExpr> to) {
        SqlExpr hit = to.get(e);
        if (hit != null) {
            return hit;
        }
        List<SqlExpr> cs = e.children();
        if (cs.isEmpty()) {
            return e;
        }
        List<SqlExpr> out = new ArrayList<>(cs.size());
        boolean changed = false;
        for (SqlExpr c : cs) {
            SqlExpr n = substitute(c, to);
            changed |= n != c;
            out.add(n);
        }
        return changed ? e.withChildren(out) : e;
    }

    private static List<SqlExpr> conjuncts(SqlExpr e) {
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            List<SqlExpr> out = new ArrayList<>();
            for (SqlExpr a : c.args()) {
                out.addAll(conjuncts(a));
            }
            return out;
        }
        if (e instanceof SqlExpr.Group g) {
            return conjuncts(g.inner());
        }
        return List.of(e);
    }

    private static void aliases(SqlSource s, Set<String> out) {
        if (s instanceof SqlSource.Join j) {
            aliases(j.left(), out);
            aliases(j.right(), out);
        } else if (s.alias() != null) {
            out.add(s.alias());
        }
    }
}
