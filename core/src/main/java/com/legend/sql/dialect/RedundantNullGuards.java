// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A WHERE's conjunct {@code c IS NOT NULL} beside a conjunct that compares {@code c} ({@code =}, {@code <>}, {@code <},
 * {@code <=}, {@code >}, {@code >=}) says nothing the comparison does not: where c is NULL the comparison is unknown,
 * and the WHERE drops the row either way. The lowering writes such guards beside each correlation of a subquery
 * ({@code u.t_id IS NOT NULL AND t.id IS NOT NULL AND u.t_id = t.id}), and Snowflake decorrelates a subquery whose
 * own WHERE reads the outer row in equalities alone: a guard of the outer column is "Unsupported subquery type cannot
 * be evaluated" there (the IQ scale program's Snowflake lane, 2026-10-23, the correlated ALL of
 * {@code m1b/all_correlated}). So for Snowflake such a guard is dropped from a subquery's own WHERE.
 */
final class RedundantNullGuards extends SqlRewriter {

    private static final Set<SqlFn> REJECTING = Set.of(SqlFn.EQUAL, SqlFn.NOT_EQUAL, SqlFn.LESS, SqlFn.LESS_EQUAL,
            SqlFn.GREATER, SqlFn.GREATER_EQUAL);

    @Override
    protected SqlExpr expr(SqlExpr e) {
        return switch (e) {
            case SqlExpr.Exists x when unguarded(x.subquery()) != x.subquery() ->
                    new SqlExpr.Exists(unguarded(x.subquery()));
            case SqlExpr.InSubquery i when unguarded(i.subquery()) != i.subquery() ->
                    new SqlExpr.InSubquery(i.value(), unguarded(i.subquery()));
            case SqlExpr.Quantified q when unguarded(q.subquery()) != q.subquery() ->
                    new SqlExpr.Quantified(q.value(), q.comparison(), q.quantifier(), unguarded(q.subquery()));
            case SqlExpr.ScalarSubquery q when unguarded(q.subquery()) != q.subquery() ->
                    new SqlExpr.ScalarSubquery(unguarded(q.subquery()));
            default -> e;
        };
    }

    /** {@code q}, a subquery, its own WHERE's redundant guards dropped (a statement's other WHEREs keep theirs: the
     *  guards matter to Snowflake in a correlated subquery alone). */
    private static SqlQuery unguarded(SqlQuery q) {
        if (!(q instanceof SqlSelect s) || s.where() == null) {
            return q;
        }
        List<SqlExpr> parts = new ArrayList<>();
        conjuncts(s.where(), parts);
        Set<Object> compared = new HashSet<>();
        for (SqlExpr p : parts) {
            if (ungrouped(p) instanceof SqlExpr.Call c && REJECTING.contains(c.fn()) && c.args().size() == 2) {
                compared.add(key(c.args().get(0)));
                compared.add(key(c.args().get(1)));
            }
        }
        List<SqlExpr> kept = new ArrayList<>();
        for (SqlExpr p : parts) {
            boolean redundant = ungrouped(p) instanceof SqlExpr.Call c && c.fn() == SqlFn.IS_NOT_NULL
                    && c.args().size() == 1 && compared.contains(key(c.args().get(0)));
            if (!redundant) {
                kept.add(p);
            }
        }
        if (kept.size() == parts.size()) {
            return s;
        }
        return new SqlSelect(s.projections(), s.distinct(), s.from(), and(kept), s.groupBy(), s.having(), s.qualify(),
                s.orderBy(), s.limit(), s.offset(), s.outputs());
    }

    /** What {@code e} is compared as: a column by its table and name (the IQ scale program, 2026-10-26: a guard's
     *  column and the comparison's carried different type facts, so the guard of the outer row stayed), else itself. */
    private static Object key(SqlExpr e) {
        return ungrouped(e) instanceof SqlExpr.Column c ? List.of(String.valueOf(c.table()), c.name()) : ungrouped(e);
    }

    private static SqlExpr ungrouped(SqlExpr e) {
        return e instanceof SqlExpr.Group g ? ungrouped(g.inner()) : e;
    }

    private static void conjuncts(SqlExpr e, List<SqlExpr> out) {
        if (ungrouped(e) instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
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
}
