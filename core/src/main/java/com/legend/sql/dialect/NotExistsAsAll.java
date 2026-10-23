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
import java.util.Map;
import java.util.Set;

/**
 * The IQ island's NOT IN and ALL, {@code NOT EXISTS (SELECT 1 FROM s WHERE [w AND] NOT (x IS NOT NULL AND y IS NOT
 * NULL AND x op y))}, as a WHERE clause's conjunct: {@code x op ALL (SELECT y FROM s [WHERE w])}, IS DISTINCT FROM's
 * op as {@code <>}. The NOT EXISTS is true exactly where the ALL is: for every row x and y are values and x op y holds
 * (over no row both are true; where x is NULL over some row, or a y is, the ALL is unknown or false and the NOT EXISTS
 * false), and a WHERE keeps the same rows of either, as it drops an unknown. Snowflake evaluates the ALL, a documented
 * subquery position, and not the NOT EXISTS, whose correlation is under a NOT ("Unsupported subquery type cannot be
 * evaluated", the IQ scale program's Snowflake lane, 2026-10-23). Anywhere but a WHERE's conjunct, where an unknown
 * and a false part ways, it stays as it is.
 */
final class NotExistsAsAll extends SqlRewriter {

    /** A comparison, and its mirror: {@code a op b} is {@code b mirror a}. */
    private static final Map<SqlFn, SqlFn> MIRROR = Map.of(SqlFn.EQUAL, SqlFn.EQUAL, SqlFn.NOT_EQUAL,
            SqlFn.NOT_EQUAL, SqlFn.IS_DISTINCT_FROM, SqlFn.NOT_EQUAL, SqlFn.NULL_SAFE_NOT_EQUAL, SqlFn.NOT_EQUAL,
            SqlFn.LESS, SqlFn.GREATER, SqlFn.LESS_EQUAL,
            SqlFn.GREATER_EQUAL, SqlFn.GREATER, SqlFn.LESS, SqlFn.GREATER_EQUAL, SqlFn.LESS_EQUAL);

    @Override
    protected SqlQuery select(SqlSelect s) {
        if (s.where() == null) {
            return s;
        }
        List<SqlExpr> conjuncts = new ArrayList<>();
        conjuncts(s.where(), conjuncts);
        boolean changed = false;
        for (int i = 0; i < conjuncts.size(); i++) {
            SqlExpr all = asAll(conjuncts.get(i));
            if (all != null) {
                conjuncts.set(i, all);
                changed = true;
            }
        }
        return changed ? new SqlSelect(s.projections(), s.distinct(), s.from(), and(conjuncts), s.groupBy(),
                s.having(), s.qualify(), s.orderBy(), s.limit(), s.offset(), s.outputs()) : s;
    }

    /** Whether {@code c} is a NOT EXISTS of the island's two-valued NOT IN or ALL (SnowflakeSubqueryRefusals). */
    static boolean isTwoValuedQuantified(SqlExpr c) {
        return asAll(c) != null;
    }

    /** {@code c} as its ALL, or null when it is no NOT EXISTS of the form. */
    private static @com.legend.base.Nullable SqlExpr asAll(SqlExpr c) {
        if (!(ungrouped(c) instanceof SqlExpr.Call not) || not.fn() != SqlFn.NOT || not.args().size() != 1
                || !(ungrouped(not.args().get(0)) instanceof SqlExpr.Exists exists)
                || !(exists.subquery() instanceof SqlSelect q) || q.where() == null || !q.groupBy().isEmpty()
                || q.having() != null || q.qualify() != null || q.limit() != null || q.offset() != null
                || q.projections().stream().anyMatch(p -> com.legend.sql.SqlAgg.aggregatesSelect(p.expr()))
                || ExistsOverOneRow.explodes(q)) {
            // an aggregate's select has one row whatever its WHERE, and an explosion's as many as its lists
            return null;
        }
        Set<String> inner = new HashSet<>();
        aliases(q.from(), inner);
        List<SqlExpr> parts = new ArrayList<>();
        conjuncts(q.where(), parts);
        for (int i = 0; i < parts.size(); i++) {
            SqlExpr.Quantified all = asAll(parts.get(i), inner, q, parts, i);
            if (all != null) {
                return all;
            }
        }
        return null;
    }

    /** The ALL of {@code q} when its WHERE's conjunct {@code part} (the {@code at}-th) is {@code NOT (x IS NOT NULL
     *  AND y IS NOT NULL AND x op y)}, x reading no column of {@code inner} and y reading one. */
    private static SqlExpr.@com.legend.base.Nullable Quantified asAll(SqlExpr part, Set<String> inner, SqlSelect q,
            List<SqlExpr> parts, int at) {
        if (!(ungrouped(part) instanceof SqlExpr.Call not) || not.fn() != SqlFn.NOT || not.args().size() != 1) {
            return null;
        }
        List<SqlExpr> three = new ArrayList<>();
        conjuncts(not.args().get(0), three);
        if (three.size() != 3) {
            return null;
        }
        SqlExpr.Call cmp = null;
        List<SqlExpr> tested = new ArrayList<>();
        for (SqlExpr e : three) {
            if (ungrouped(e) instanceof SqlExpr.Call k && k.fn() == SqlFn.IS_NOT_NULL && k.args().size() == 1) {
                tested.add(ungrouped(k.args().get(0)));
            } else if (ungrouped(e) instanceof SqlExpr.Call k && MIRROR.containsKey(k.fn()) && k.args().size() == 2) {
                cmp = k;
            }
        }
        if (cmp == null || tested.size() != 2) {
            return null;
        }
        SqlExpr a = ungrouped(cmp.args().get(0));
        SqlExpr b = ungrouped(cmp.args().get(1));
        if (!(tested.contains(a) && tested.contains(b))) {
            return null;
        }
        boolean aOuter = !reads(a, inner);
        boolean bOuter = !reads(b, inner);
        if (aOuter == bOuter) {
            return null;
        }
        SqlExpr x = aOuter ? a : b;
        SqlExpr y = aOuter ? b : a;
        // IS DISTINCT FROM, both sides values, is <>
        SqlFn op = cmp.fn() == SqlFn.IS_DISTINCT_FROM || cmp.fn() == SqlFn.NULL_SAFE_NOT_EQUAL ? SqlFn.NOT_EQUAL
                : aOuter ? cmp.fn() : java.util.Objects.requireNonNull(MIRROR.get(cmp.fn()), "mirror");
        List<SqlExpr> rest = new ArrayList<>(parts);
        rest.remove(at);
        SqlSelect ys = new SqlSelect(List.of(new SqlSelect.Projection(y, null, null)), false, q.from(),
                rest.isEmpty() ? null : and(rest), List.of(), null, null, List.of(), null, null, List.of());
        return new SqlExpr.Quantified(x, op, SqlExpr.Quantifier.ALL, ys);
    }

    private static SqlExpr ungrouped(SqlExpr e) {
        return e instanceof SqlExpr.Group g ? ungrouped(g.inner()) : e;
    }

    private static void conjuncts(SqlExpr e, List<SqlExpr> out) {
        SqlExpr u = ungrouped(e);
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

    /** Whether {@code e} reads a column of one of {@code aliases} (an unqualified one counts), its subqueries'
     *  included. */
    private static boolean reads(SqlExpr e, Set<String> aliases) {
        boolean[] found = {false};
        new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr c) {
                found[0] |= c instanceof SqlExpr.Column col && (col.table() == null || aliases.contains(col.table()));
                return c;
            }

            SqlExpr run(SqlExpr root) {
                return rewriteExpr(root);
            }
        }.run(e);
        return found[0];
    }
}
