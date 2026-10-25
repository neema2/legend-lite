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
import java.util.regex.Pattern;

/**
 * A comparison the lowering makes two-valued by its operands' guards, {@code CASE WHEN g IS NULL THEN FALSE ELSE (...
 * AND g IS NOT NULL AND ... AND a op g) END}, is its ELSE: where g is NULL the ELSE's conjunct {@code g IS NOT NULL}
 * is false, and so is the ELSE, whatever its other conjuncts are; the ELSE is never unknown (each operand it compares
 * is guarded). And a WHERE's or a join condition's conjunct {@code s IS NOT NULL}, s a scalar subquery, beside a
 * conjunct that compares s ({@code =}, {@code <>}, {@code <}, {@code <=}, {@code >}, {@code >=}), says nothing the
 * comparison does not: the row is dropped, or not matched, either way.
 *
 * <p>The IQ scale program, 2026-10-26 (the Snowflake lane at ac51fd9d, 9 runs "Unsupported subquery type cannot be
 * evaluated", 4 of them answered at 476bb731): Snowflake evaluates no correlated subquery inside a CASE -- {@code
 * a.v = (SELECT MAX(b.v) FROM g b WHERE b.k = a.k)} was written with its subquery inside the guard's CASE, and an
 * EXISTS's correlated comparison of a grouped key inside one -- and decorrelates the plain comparison. A CASE whose
 * ELSE holds an operation that may raise outside its subqueries (a division, a cast) is kept: the CASE does not
 * compute it where g is NULL.
 */
final class GuardedComparisonsUnwrapped extends SqlRewriter {

    private static final Set<SqlFn> REJECTING = Set.of(SqlFn.EQUAL, SqlFn.NOT_EQUAL, SqlFn.LESS, SqlFn.LESS_EQUAL,
            SqlFn.GREATER, SqlFn.GREATER_EQUAL);

    private static final Set<SqlFn> RAISING = Set.of(SqlFn.DIVIDE, SqlFn.INT_DIVIDE, SqlFn.MOD, SqlFn.REM);

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlExpr.Case k && k.whens().size() == 1 && k.otherwise() instanceof SqlExpr otherwise
                && ungrouped(k.whens().get(0).condition()) instanceof SqlExpr.Call isNull
                && isNull.fn() == SqlFn.IS_NULL && isNull.args().size() == 1
                && ungrouped(k.whens().get(0).then()) instanceof SqlExpr.BoolLit b && !b.value()) {
            String g = canonical(ungrouped(isNull.args().get(0)));
            List<SqlExpr> parts = new ArrayList<>();
            conjuncts(otherwise, parts);
            boolean guarded = parts.stream().anyMatch(p -> ungrouped(p) instanceof SqlExpr.Call c
                    && c.fn() == SqlFn.IS_NOT_NULL && c.args().size() == 1
                    && canonical(ungrouped(c.args().get(0))).equals(g));
            if (guarded && !raises(otherwise)) {
                return otherwise;
            }
        }
        return e;
    }

    @Override
    protected SqlQuery select(SqlSelect s) {
        SqlExpr where = s.where();
        if (where == null) {
            return s;
        }
        SqlExpr kept = filtered(where);
        return kept == where ? s : new SqlSelect(s.projections(), s.distinct(), s.from(), kept, s.groupBy(),
                s.having(), s.qualify(), s.orderBy(), s.limit(), s.offset(), s.outputs());
    }

    @Override
    protected SqlSource source(SqlSource s) {
        if (s instanceof SqlSource.Join j && j.on() instanceof SqlExpr on) {
            SqlExpr kept = filtered(on);
            if (kept != on) {
                return new SqlSource.Join(j.left(), j.right(), j.kind(), kept, j.outputs());
            }
        }
        return s;
    }

    /** {@code condition}, a WHERE's or a join's, without its conjuncts {@code s IS NOT NULL} of a scalar subquery
     *  another conjunct compares; itself where there is none. */
    private static SqlExpr filtered(SqlExpr condition) {
        List<SqlExpr> parts = new ArrayList<>();
        conjuncts(condition, parts);
        Set<String> compared = new HashSet<>();
        for (SqlExpr p : parts) {
            if (ungrouped(p) instanceof SqlExpr.Call c && REJECTING.contains(c.fn()) && c.args().size() == 2) {
                for (SqlExpr a : c.args()) {
                    if (ungrouped(a) instanceof SqlExpr.ScalarSubquery q) {
                        compared.add(canonical(q));
                    }
                }
            }
        }
        List<SqlExpr> kept = new ArrayList<>();
        for (SqlExpr p : parts) {
            boolean redundant = ungrouped(p) instanceof SqlExpr.Call c && c.fn() == SqlFn.IS_NOT_NULL
                    && c.args().size() == 1 && ungrouped(c.args().get(0)) instanceof SqlExpr.ScalarSubquery q
                    && compared.contains(canonical(q));
            if (!redundant) {
                kept.add(p);
            }
        }
        if (kept.size() == parts.size()) {
            return condition;
        }
        SqlExpr out = kept.get(0);
        for (int i = 1; i < kept.size(); i++) {
            out = SqlExpr.Call.of(SqlFn.AND, out, kept.get(i));
        }
        return out;
    }

    /** Whether {@code e} holds, outside its subqueries, an operation that may raise: a division, a remainder, a
     *  cast. */
    private static boolean raises(SqlExpr e) {
        if (e instanceof SqlExpr.Cast || e instanceof SqlExpr.Call c && RAISING.contains(c.fn())) {
            return true;
        }
        for (SqlExpr child : e.children()) {
            if (raises(child)) {
                return true;
            }
        }
        return false;
    }

    /** {@code e}'s text, each alias a source inside it defines numbered by where it first appears: two copies of one
     *  operand, which the lowering writes with fresh aliases each, read alike (the aliases of the queries around them,
     *  which they correlate to, are kept). */
    private static String canonical(SqlExpr e) {
        List<String> defined = new ArrayList<>();
        new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (!(s instanceof SqlSource.Join) && !(s instanceof SqlSource.Dual) && s.alias() instanceof String a
                        && !defined.contains(a)) {
                    defined.add(a);
                }
                return s;
            }

            SqlExpr run(SqlExpr root) {
                return rewriteExpr(root);
            }
        }.run(e);
        String text = e.toString();
        for (int i = 0; i < defined.size(); i++) {
            text = text.replaceAll("\\b" + Pattern.quote(defined.get(i)) + "\\b",
                    java.util.regex.Matcher.quoteReplacement("\u0000" + i + "\u0000"));
        }
        return text;
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
}
