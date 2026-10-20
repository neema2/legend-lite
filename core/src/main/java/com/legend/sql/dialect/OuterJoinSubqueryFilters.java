// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * DuckDB cannot plan a scalar subquery in a LEFT JOIN's condition ("Cannot perform non-inner join on subquery"). A
 * conjunct of the condition that reads no column of the join's left side reads the null-supplying side alone, so it
 * filters that side before the join: {@code l LEFT JOIN r ON a AND c} is {@code l LEFT JOIN (SELECT * FROM r WHERE c) r
 * ON a} -- a right row failing {@code c} matches no left row either way (IQ island, 2026-10-17: IQ reads a subquery in
 * an outer join's condition only in such a conjunct, beside a condition of a column, {@code ON u.t_id > 0 AND u.id =
 * (SELECT MAX(id) FROM u)}). Only conjuncts holding a scalar subquery move; a right side that is a table or a subselect
 * keeps its alias, inside and out.
 */
final class OuterJoinSubqueryFilters extends SqlRewriter {

    @Override
    protected SqlQuery select(SqlSelect s) {
        SqlSource from = moved(s.from());
        return from == s.from() ? s : new SqlSelect(s.projections(), s.distinct(), from, s.where(), s.groupBy(),
                s.having(), s.qualify(), s.orderBy(), s.limit(), s.offset(), s.outputs());
    }

    private static SqlSource moved(SqlSource source) {
        if (!(source instanceof SqlSource.Join j)) {
            return source;
        }
        SqlSource left = moved(j.left());
        SqlSource right = moved(j.right());
        SqlExpr on = j.on();
        if (j.kind() == SqlSource.Join.Kind.LEFT && on != null
                && (right instanceof SqlSource.Table || right instanceof SqlSource.Subselect)) {
            Set<String> leftAliases = new HashSet<>();
            aliases(left, leftAliases);
            List<SqlExpr> conjuncts = new ArrayList<>();
            conjuncts(on, conjuncts);
            List<SqlExpr> kept = new ArrayList<>();
            List<SqlExpr> filters = new ArrayList<>();
            for (SqlExpr c : conjuncts) {
                (holdsScalarSubquery(c) && !readsAny(c, leftAliases) ? filters : kept).add(c);
            }
            if (!filters.isEmpty() && !kept.isEmpty()) {
                SqlSelect filtered = new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Star(null), null,
                        null)), false, right, and(filters), List.of(), null, null, List.of(), null, null,
                        right.outputs());
                return new SqlSource.Join(left, new SqlSource.Subselect(filtered, right.alias(), null), j.kind(),
                        and(kept));
            }
        }
        return left == j.left() && right == j.right() ? j : new SqlSource.Join(left, right, j.kind(), on);
    }

    private static void aliases(SqlSource s, Set<String> out) {
        if (s instanceof SqlSource.Join j) {
            aliases(j.left(), out);
            aliases(j.right(), out);
        } else {
            out.add(s.alias());
        }
    }

    private static void conjuncts(SqlExpr e, List<SqlExpr> out) {
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            c.args().forEach(a -> conjuncts(a, out));
        } else if (e instanceof SqlExpr.Group g) {
            conjuncts(g.inner(), out);
        } else {
            out.add(e);
        }
    }

    private static SqlExpr and(List<SqlExpr> parts) {
        SqlExpr out = parts.get(0);
        for (int i = 1; i < parts.size(); i++) {
            out = SqlExpr.Call.of(SqlFn.AND, out, parts.get(i));
        }
        return out;
    }

    private static boolean holdsScalarSubquery(SqlExpr e) {
        boolean[] found = {false};
        new Walk() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                found[0] |= x instanceof SqlExpr.ScalarSubquery;
                return x;
            }
        }.run(e);
        return found[0];
    }

    /** Whether {@code e} reads a column of one of {@code aliases} anywhere, or an unqualified one outside its
     *  subqueries. */
    private static boolean readsAny(SqlExpr e, Set<String> aliases) {
        boolean[] found = {false};
        new Walk() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                if (x instanceof SqlExpr.Column c && (c.table() == null || aliases.contains(c.table()))) {
                    found[0] = true;
                }
                return x;
            }
        }.run(e);
        return found[0];
    }

    /** An expression walk with the rewriter's own traversal (post-children hooks, into subqueries). */
    private abstract static class Walk extends SqlRewriter {
        final SqlExpr run(SqlExpr e) {
            return rewriteExpr(e);
        }
    }
}
