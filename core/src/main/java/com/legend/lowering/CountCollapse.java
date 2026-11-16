// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.List;

/**
 * A RELATION'S SIZE COUNTS ITS ROWS, WITH NO WRAPPER (the DuckDB plan's step 1, the lean program, 2026-11-16): a
 * select of {@code COUNT(*)} alone over a derived table that keeps its source's rows counts the source itself, and a
 * FROM-less root whose one value is a one-row aggregate's subquery is that aggregate's select.
 *
 * <pre>
 *   SELECT (SELECT COUNT(*) FROM (SELECT t.a, t.b FROM t WHERE p) AS t0) AS value
 *   SELECT COUNT(*) AS value FROM t WHERE p
 * </pre>
 *
 * <p>The derived table keeps its source's rows when it has no DISTINCT, GROUP BY, HAVING, QUALIFY, LIMIT or OFFSET, no
 * aggregate (one row over all) and no set-returning projection; its WHERE stays, its projections and ORDER BY go (a
 * count reads neither). A projection that can raise keeps the wrapper: the database may evaluate it, and a raise is
 * never dropped (the laziness rule's other side). The same query, smaller, on every database.
 */
final class CountCollapse {

    private CountCollapse() {
    }

    static SqlQuery apply(SqlQuery q) {
        SqlQuery inner = new SqlRewriter() {
            @Override
            protected SqlQuery select(SqlSelect s) {
                return countOverRows(s);
            }
        }.rewriteRoot(q);
        return rootValue(inner);
    }

    /** {@code SELECT COUNT(*) FROM (rows) AS t} as {@code SELECT COUNT(*) FROM <rows' source> WHERE <rows' filter>}. */
    private static SqlSelect countOverRows(SqlSelect s) {
        if (!(s.from() instanceof SqlSource.Subselect sub) || !(sub.inner() instanceof SqlSelect rows)
                || s.distinct() || s.where() != null || !s.groupBy().isEmpty() || s.having() != null
                || s.qualify() != null || !s.orderBy().isEmpty() || s.limit() != null || s.offset() != null
                || s.projections().isEmpty()
                || !s.projections().stream().allMatch(p -> countStar(p.expr()))) {
            return s;
        }
        if (!keepsRows(rows)) {
            return s;
        }
        return new SqlSelect(s.projections(), false, rows.from(), rows.where(), List.of(), null, null, List.of(), null,
                null, s.outputs());
    }

    private static boolean countStar(SqlExpr e) {
        return e instanceof SqlAgg.Reducer r && r.fn() == SqlAgg.Fn.COUNT && r.args().isEmpty() && !r.distinct()
                && r.orderBy().isEmpty();
    }

    /** Whether {@code rows} gives one row per row of its FROM that its WHERE keeps, and dropping its projections
     *  drops no raise. */
    private static boolean keepsRows(SqlSelect rows) {
        if (rows.distinct() || !rows.groupBy().isEmpty() || rows.having() != null || rows.qualify() != null
                || rows.limit() != null || rows.offset() != null || rows.from() instanceof SqlSource.Dual
                || rows.projections().isEmpty() || Fold.unnestInProjections(rows)) {
            return false;
        }
        for (SqlSelect.Projection p : rows.projections()) {
            if (aggregates(p.expr()) || LetBinding.canRaise(p.expr())) {
                return false;
            }
        }
        return true;
    }

    private static boolean aggregates(SqlExpr e) {
        boolean[] found = {false};
        LetBinding.mapExpr(e, x -> {
            if (x instanceof SqlAgg.Reducer || x instanceof SqlExpr.OrderedListAgg || x instanceof SqlExpr.JsonArrayAgg) {
                found[0] = true;
                return x;
            }
            // a subquery's own aggregates are its own: not this select's
            if (x instanceof SqlExpr.ScalarSubquery || x instanceof SqlExpr.Exists || x instanceof SqlExpr.InSubquery
                    || x instanceof SqlExpr.Quantified) {
                return x;
            }
            return null;
        });
        return found[0];
    }

    /** {@code SELECT (SELECT agg FROM ...) AS value} with no FROM as {@code SELECT agg AS value FROM ...}: an aggregate
     *  with no GROUP BY is one row, so the scalar subquery's row is the root's. */
    private static SqlQuery rootValue(SqlQuery q) {
        if (!(q instanceof SqlSelect root) || !(root.from() instanceof SqlSource.Dual) || root.projections().size() != 1
                || root.where() != null || !root.groupBy().isEmpty() || root.having() != null || root.qualify() != null
                || !root.orderBy().isEmpty() || root.limit() != null || root.offset() != null || root.distinct()) {
            return q;
        }
        SqlSelect.Projection only = root.projections().get(0);
        if (!(only.expr() instanceof SqlExpr.ScalarSubquery sq) || !(sq.subquery() instanceof SqlSelect agg)
                || agg.projections().size() != 1 || !agg.groupBy().isEmpty() || agg.having() != null
                || agg.qualify() != null || agg.distinct() || agg.limit() != null || agg.offset() != null
                || agg.from() instanceof SqlSource.Dual || !(agg.projections().get(0).expr() instanceof SqlAgg.Reducer)) {
            return q;
        }
        SqlExpr value = agg.projections().get(0).expr();
        return new SqlSelect(List.of(new SqlSelect.Projection(value, only.alias(), only.out())), false, agg.from(),
                agg.where(), List.of(), null, null, List.of(), null, null, root.outputs());
    }
}
