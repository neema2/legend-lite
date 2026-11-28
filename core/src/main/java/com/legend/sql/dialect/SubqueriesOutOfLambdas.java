// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A SUB-QUERY A LIST'S FUNCTION HOLDS, READ ONCE OUTSIDE IT (step 6B part 2, 2026-11-28): DuckDB runs no sub-query
 * inside a lambda ("Binder Error: subqueries in lambda expressions are not supported", 1.5.6), so a value that is a
 * statement of its own -- a comparison of two tables, of two lists of dates held two ways, a side read whole --
 * written inside a list's function was refused by name. Such a sub-query reads nothing of the lambda's element (the
 * planner plans a statement only for sides that read none), so it is one value however many elements there are: the
 * call is written as a transform of a one-element list that holds the sub-queries' values, its lambda's parameter
 * read inside the original lambdas where each sub-query stood --
 * {@code list_transform([{s0: (sub-query)}], __held -> list_filter(xs, x -> ... __held.s0 ...))[1]}. A dialect's own
 * decomposition (AGENTS.md invariant 3); the plan is untouched. A sub-query that reads a lambda's parameter has no
 * such form and stays refused.
 *
 * <p>One difference from a database that runs the sub-query where it stands: it is evaluated once even where the
 * list has no element, so a comparison whose side raises is raised over an empty list too.
 */
final class SubqueriesOutOfLambdas {

    private static final String HELD = "__held";

    private SubqueriesOutOfLambdas() {
    }

    /** What DuckDB WRITES of {@code e}: of the "same values" instruction its two list values alone, of a membership
     *  its needle and its list value; the sides' rows that ride with either, for the dialects that compare rows,
     *  are never written here. */
    static SqlExpr written(SqlExpr e) {
        SqlExpr inner = e.mapChildren(SubqueriesOutOfLambdas::written);
        return inner instanceof SqlExpr.Call c && c.args().size() == 4
                && (c.fn() == SqlFn.SAME_IN_ORDER || c.fn() == SqlFn.SAME_IN_ANY_ORDER)
                ? new SqlExpr.Call(c.fn(), c.args().subList(0, 2))
                : inner instanceof SqlExpr.Membership m && m.rows() != null
                ? new SqlExpr.Membership(m.needle(), m.collection()) : inner;
    }

    /** Whether {@code e}, as DuckDB writes it, holds a sub-query. */
    static boolean holdsASubquery(SqlExpr e) {
        return subquery(e) || e.children().stream().anyMatch(SubqueriesOutOfLambdas::holdsASubquery);
    }

    private static boolean subquery(SqlExpr e) {
        return e instanceof SqlExpr.ScalarSubquery || e instanceof SqlExpr.Exists || e instanceof SqlExpr.InSubquery
                || e instanceof SqlExpr.Quantified;
    }

    /**
     * {@code call}, a call one of whose arguments is a lambda that holds a sub-query, with each such sub-query read
     * once outside the lambdas; null where it holds none, or one of them has no such form (it reads a lambda's
     * parameter, or is a test of a value against a sub-query's rows): the caller refuses those by name.
     */
    static @com.legend.base.Nullable SqlExpr of(SqlExpr.Call call) {
        if (call.args().stream().noneMatch(a -> a instanceof SqlExpr.Lambda)) {
            return null;
        }
        SqlExpr written = written(call);
        if (!(written instanceof SqlExpr.Call c) || c.args().stream().noneMatch(a -> a instanceof SqlExpr.Lambda l
                && holdsASubquery(l.body()))) {
            return null;
        }
        Set<String> parameters = new HashSet<>();
        parametersOf(c, parameters);
        List<SqlExpr> held = new ArrayList<>();
        boolean[] noForm = {false};
        SqlExpr inside = outside(c, false, held, parameters, noForm);
        if (held.isEmpty() || noForm[0]) {
            return null;
        }
        List<SqlExpr.StructLit.Field> fields = new ArrayList<>();
        for (int i = 0; i < held.size(); i++) {
            fields.add(new SqlExpr.StructLit.Field("s" + i, held.get(i)));
        }
        return SqlExpr.Call.of(SqlFn.LIST_GET, SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                new SqlExpr.ArrayLit(List.of(new SqlExpr.StructLit(fields))),
                new SqlExpr.Lambda(List.of(HELD), inside)), new SqlExpr.IntLit(1));
    }

    private static void parametersOf(SqlExpr e, Set<String> into) {
        if (e instanceof SqlExpr.Lambda l) {
            into.addAll(l.params());
        }
        e.children().forEach(c -> parametersOf(c, into));
    }

    private static SqlExpr outside(SqlExpr e, boolean inALambda, List<SqlExpr> held, Set<String> parameters,
            boolean[] noForm) {
        if (inALambda && subquery(e)) {
            if (!(e instanceof SqlExpr.ScalarSubquery || e instanceof SqlExpr.Exists) || reads(e, parameters)) {
                noForm[0] = true;
                return e;
            }
            held.add(e);
            return new SqlExpr.StructGet(SqlExpr.Column.derived(null, HELD), "s" + (held.size() - 1));
        }
        boolean within = inALambda || e instanceof SqlExpr.Lambda;
        return e.mapChildren(c -> outside(c, within, held, parameters, noForm));
    }

    /** Whether the sub-query {@code e} reads one of the lambdas' parameters: a column with no table, of that name. */
    private static boolean reads(SqlExpr e, Set<String> parameters) {
        boolean[] hit = {false};
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr x) {
                if (x instanceof SqlExpr.Column col && col.table() == null && parameters.contains(col.name())) {
                    hit[0] = true;
                }
                return x;
            }

            void scan(SqlExpr x) {
                rewriteExpr(x);
            }
        }.scan(e);
        return hit[0];
    }
}
