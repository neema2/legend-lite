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
 * A correlated scalar subquery that reads a value of no aggregate, {@code (SELECT u.label FROM u WHERE u.id = t.id)},
 * is the one value of its rows: {@code (SELECT CASE WHEN COUNT(*) > 1 THEN error ELSE ANY_VALUE(u.label) END FROM u
 * WHERE u.id = t.id)} -- NULL over no row, the value over one, and raised over more, as a scalar subquery is. Snowflake
 * evaluates a correlated scalar subquery only of an aggregate ("Unsupported subquery type cannot be evaluated"; the IQ
 * scale program's Snowflake lane, 2026-10-23, measured). A subquery that groups, sorts, limits or is DISTINCT stays as
 * it is; so does one that reads only its own rows, which Snowflake evaluates as written.
 */
final class ScalarSubqueriesAggregated extends SqlRewriter {

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlExpr.ScalarSubquery s && s.subquery() instanceof SqlSelect q && oneValueOfItsRows(q)
                && correlated(q)) {
            SqlSelect.Projection p = q.projections().get(0);
            if (oneGroup(q)) {
                // one group at the most: its value, with no count (Snowflake's internal error 370001 over a grouped
                // derived table, measured by the sibling, 2026-10-23; ANY_VALUE alone answered)
                return new SqlExpr.ScalarSubquery(new SqlSelect(List.of(new SqlSelect.Projection(
                        SqlAgg.Reducer.of(SqlAgg.Fn.ANY_VALUE, p.expr()), p.alias(), p.out())), false, q.from(),
                        q.where(), List.of(), null, null, List.of(), null, null, q.outputs()));
            }
            SqlExpr value = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.GREATER, SqlAgg.Reducer.of(SqlAgg.Fn.COUNT), new SqlExpr.IntLit(1)),
                    SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit(
                            "More than one row returned by a subquery used as an expression")))),
                    SqlAgg.Reducer.of(SqlAgg.Fn.ANY_VALUE, p.expr()), com.legend.sql.SqlTyping.UNKNOWN);
            return new SqlExpr.ScalarSubquery(new SqlSelect(List.of(new SqlSelect.Projection(value, p.alias(),
                    p.out())), false, q.from(), q.where(), List.of(), null, null, List.of(), null, null,
                    q.outputs()));
        }
        return e;
    }

    /** One projection of no aggregate or window, over rows nothing groups, sorts, limits or makes distinct. */
    private static boolean oneValueOfItsRows(SqlSelect q) {
        return q.projections().size() == 1 && !(q.projections().get(0).expr() instanceof SqlExpr.Star)
                && !(q.projections().get(0).expr() instanceof SqlExpr.StarExcept) && !ExistsOverOneRow.explodes(q)
                && !SqlAgg.aggregatesSelect(q.projections().get(0).expr())
                && !windowed(q.projections().get(0).expr()) && !q.distinct() && q.groupBy().isEmpty()
                && q.having() == null && q.qualify() == null && q.orderBy().isEmpty() && q.limit() == null
                && q.offset() == null;
    }

    /** Whether {@code q} reads one group of a grouped derived table at the most: its WHERE equates each of the table's
     *  group keys, as the table shows it, to a value of no column of the table's (a correlation or a constant). A
     *  group's key is its own, so no two rows match (the IQ scale program's Q24, 2026-10-23: WITH q AS (SELECT t_id,
     *  COUNT(*) AS n FROM u GROUP BY t_id) ... (SELECT n FROM q WHERE q.t_id = t.id)). */
    private static boolean oneGroup(SqlSelect q) {
        if (!(q.from() instanceof SqlSource.Subselect sub) || !(sub.inner() instanceof SqlSelect g)
                || g.groupBy().isEmpty() || q.where() == null) {
            return false;
        }
        List<SqlExpr> conjuncts = new ArrayList<>();
        conjuncts(q.where(), conjuncts);
        Set<String> own = Set.of(sub.alias());
        for (SqlExpr key : g.groupBy()) {
            String shown = null;
            for (SqlSelect.Projection p : g.projections()) {
                if (sameKey(p.expr(), key)) {
                    shown = p.alias() != null ? p.alias() : p.expr() instanceof SqlExpr.Column c ? c.name() : null;
                    break;
                }
            }
            if (shown == null || !fixed(conjuncts, sub.alias(), shown, own)) {
                return false;
            }
        }
        return true;
    }

    /** Whether a projection is a group key: the same column (whatever its stamped type), or an equal expression. */
    private static boolean sameKey(SqlExpr projected, SqlExpr key) {
        if (projected instanceof SqlExpr.Column a && key instanceof SqlExpr.Column b) {
            return java.util.Objects.equals(a.table(), b.table()) && a.name().equals(b.name());
        }
        return projected.equals(key);
    }

    /** Whether a conjunct equates {@code alias.column} to a value reading none of {@code own}'s tables. */
    private static boolean fixed(List<SqlExpr> conjuncts, String alias, String column, Set<String> own) {
        for (SqlExpr c : conjuncts) {
            if (c instanceof SqlExpr.Call eq && eq.fn() == SqlFn.EQUAL && eq.args().size() == 2) {
                for (int i = 0; i < 2; i++) {
                    if (eq.args().get(i) instanceof SqlExpr.Column col && alias.equals(col.table())
                            && column.equals(col.name()) && !readsOwn(eq.args().get(1 - i), own)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean readsOwn(SqlExpr e, Set<String> own) {
        if (e instanceof SqlExpr.Column c && (c.table() == null || own.contains(c.table()))) {
            return true;
        }
        if (e instanceof SqlExpr.ScalarSubquery || e instanceof SqlExpr.Exists || e instanceof SqlExpr.InSubquery
                || e instanceof SqlExpr.Quantified) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (readsOwn(c, own)) {
                return true;
            }
        }
        return false;
    }

    private static void conjuncts(SqlExpr e, List<SqlExpr> out) {
        SqlExpr x = e instanceof SqlExpr.Group g ? g.inner() : e;
        if (x instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            c.args().forEach(a -> conjuncts(a, out));
        } else {
            out.add(x);
        }
    }

    private static boolean windowed(SqlExpr e) {
        if (e instanceof SqlExpr.WindowCall) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (windowed(c)) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code q}'s value or WHERE reads a column of a table its FROM does not name. */
    private static boolean correlated(SqlSelect q) {
        Set<String> own = new HashSet<>();
        aliases(q.from(), own);
        return readsOther(q.projections().get(0).expr(), own) || q.where() != null && readsOther(q.where(), own);
    }

    private static void aliases(SqlSource s, Set<String> out) {
        if (s instanceof SqlSource.Join j) {
            aliases(j.left(), out);
            aliases(j.right(), out);
        } else if (!(s instanceof SqlSource.Dual) && s.alias() != null) {
            out.add(s.alias());
        }
    }

    private static boolean readsOther(SqlExpr e, Set<String> own) {
        if (e instanceof SqlExpr.Column c && c.table() != null && !own.contains(c.table())) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (readsOther(c, own)) {
                return true;
            }
        }
        return false;
    }
}
