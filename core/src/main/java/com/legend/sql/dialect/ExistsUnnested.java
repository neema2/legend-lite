// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
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
 * An EXISTS asks only whether its subquery has a row, so for Snowflake, which decorrelates an EXISTS whose correlation
 * sits in the subquery's own WHERE as equalities (the IQ scale program's Snowflake lane, 2026-10-23: "Unsupported
 * subquery type cannot be evaluated" otherwise), it is written closer to that:
 * <ul>
 *   <li>a select that only passes a derived table's rows on is that derived table: {@code EXISTS (SELECT 1 FROM (q) t
 *       WHERE TRUE)} is {@code EXISTS (q)};</li>
 *   <li>a row limit of one or more and its order decide nothing: {@code EXISTS (SELECT ... LIMIT 1)} is the select
 *       without them; with an offset m, the select has a row past m: {@code (SELECT COUNT(*) ...) > m};</li>
 *   <li>a condition that reads no column of the subquery's own is the same for each of its rows:
 *       {@code EXISTS (SELECT ... WHERE w AND p)} is {@code COALESCE(p, FALSE) AND EXISTS (SELECT ... WHERE w)}.</li>
 * </ul>
 */
final class ExistsUnnested extends SqlRewriter {

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (!(e instanceof SqlExpr.Exists x)) {
            return e;
        }
        SqlQuery q = x.subquery();
        while (q instanceof SqlSelect s && s.from() instanceof SqlSource.Subselect sub && passesRowsOn(s)
                && !(sub.inner() instanceof SqlSelect in && ExistsOverOneRow.explodes(in))) {
            q = sub.inner();
        }
        if (q instanceof SqlSelect s && ExistsOverOneRow.explodes(s)) {
            return q == x.subquery() ? e : new SqlExpr.Exists(q);
        }
        if (q instanceof SqlSelect s && s.limit() != null && s.limit() >= 1 && s.qualify() == null) {
            if (s.offset() == null) {
                q = new SqlSelect(s.projections(), s.distinct(), s.from(), s.where(), s.groupBy(), s.having(), null,
                        List.of(), null, null, s.outputs());
            } else if (!s.distinct() && s.groupBy().isEmpty() && s.having() == null && !aggregates(s)) {
                SqlSelect count = new SqlSelect(List.of(new SqlSelect.Projection(SqlAgg.Reducer.of(SqlAgg.Fn.COUNT),
                        null, null)), false, s.from(), s.where(), List.of(), null, null, List.of(), null, null,
                        List.of());
                return SqlExpr.Call.of(SqlFn.GREATER, new SqlExpr.ScalarSubquery(count),
                        new SqlExpr.IntLit(s.offset()));
            }
        }
        if (q instanceof SqlSelect s && s.where() != null && s.groupBy().isEmpty() && s.having() == null
                && s.qualify() == null && s.limit() == null && s.offset() == null && !aggregates(s)) {
            Set<String> own = new HashSet<>();
            aliases(s.from(), own);
            List<SqlExpr> parts = new ArrayList<>();
            conjuncts(s.where(), parts);
            List<SqlExpr> outer = new ArrayList<>();
            List<SqlExpr> rest = new ArrayList<>();
            for (SqlExpr p : parts) {
                if (!(p instanceof SqlExpr.BoolLit b && b.value())) {
                    (readsNone(p, own) ? outer : rest).add(p);
                }
            }
            if (!outer.isEmpty()) {
                SqlSelect inner = new SqlSelect(s.projections(), s.distinct(), s.from(),
                        rest.isEmpty() ? null : and(rest), List.of(), null, null, s.orderBy(), null, null,
                        s.outputs());
                return SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.COALESCE, and(outer),
                        new SqlExpr.BoolLit(false)), new SqlExpr.Exists(inner));
            }
        }
        return q == x.subquery() ? e : new SqlExpr.Exists(q);
    }

    /** {@code s} keeps its derived table's rows, one for one: no filter but TRUE, no grouping, limit or aggregate. */
    private static boolean passesRowsOn(SqlSelect s) {
        return (s.where() == null || s.where() instanceof SqlExpr.BoolLit b && b.value()) && s.groupBy().isEmpty()
                && s.having() == null && s.qualify() == null && s.limit() == null && s.offset() == null
                && !aggregates(s);
    }

    private static boolean aggregates(SqlSelect s) {
        return s.projections().stream().anyMatch(p -> SqlAgg.aggregatesSelect(p.expr()));
    }

    private static void conjuncts(SqlExpr e, List<SqlExpr> out) {
        SqlExpr u = e instanceof SqlExpr.Group g ? g.inner() : e;
        if (u instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            c.args().forEach(a -> conjuncts(a, out));
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

    private static void aliases(SqlSource s, Set<String> out) {
        if (s instanceof SqlSource.Join j) {
            aliases(j.left(), out);
            aliases(j.right(), out);
        } else if (!(s instanceof SqlSource.Dual) && s.alias() != null) {
            out.add(s.alias());
        }
    }

    /** Whether {@code e} reads no column of {@code own} (an unqualified one is taken as own), and holds no subquery
     *  (whose own correlation is its own question). */
    private static boolean readsNone(SqlExpr e, Set<String> own) {
        boolean[] found = {false};
        new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr c) {
                found[0] |= c instanceof SqlExpr.Column col && (col.table() == null || own.contains(col.table()))
                        || c instanceof SqlExpr.Exists || c instanceof SqlExpr.InSubquery
                        || c instanceof SqlExpr.ScalarSubquery || c instanceof SqlExpr.Quantified;
                return c;
            }

            SqlExpr run(SqlExpr root) {
                return rewriteExpr(root);
            }
        }.run(e);
        return !found[0];
    }
}
