// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.AliasPrefix;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A relation the statement reads more than once, written back where each read stands (2026-11-04): the lowering
 * writes it once, a common table expression {@code __let<n>} ({@link SqlWith#SHARED_PREFIX}) at the statement's head
 * that each read names ({@code lowering.SharedRelations}). This pass is for a database that cannot hold it there:
 * MongoDB's pipeline has no common table expressions ({@link #all}), and H2 loses a placeholder's value inside a common
 * table expression another one reads ({@link #bindingAValue}; the defect {@link GridReportCtesInline} exists for,
 * {@code H2LostCteParameterTest}). Such an expression is a derived table at each read, as the lowering wrote it before
 * it was shared, and the statement holds as many copies as reads. Each copy's aliases are prefixed ({@link
 * AliasPrefix}): a statement's aliases are unique ({@code SourceSpelling}). An expression read by another written in
 * place is written in place inside it first, in the WITH's order.
 */
final class SharedRelationsInPlace extends SqlRewriter {

    private final Predicate<SqlQuery> which;
    private final int[] copies = {0};

    private SharedRelationsInPlace(Predicate<SqlQuery> which) {
        this.which = which;
    }

    /** Every shared relation written in place: a database with no common table expressions. */
    static SharedRelationsInPlace all() {
        return new SharedRelationsInPlace(q -> true);
    }

    /** Each shared relation whose query binds a placeholder written in place (H2). */
    static SharedRelationsInPlace bindingAValue() {
        return new SharedRelationsInPlace(SharedRelationsInPlace::binds);
    }

    /** Every WITH, at any depth, after its parts are rewritten. */
    @Override
    protected SqlQuery with(SqlWith w) {
        Map<String, SqlQuery> inPlace = new HashMap<>();
        List<SqlWith.Cte> kept = new ArrayList<>();
        for (SqlWith.Cte c : w.ctes()) {
            SqlQuery query = substituted(c.query(), inPlace);
            if (c.name().startsWith(SqlWith.SHARED_PREFIX) && which.test(query)) {
                inPlace.put(c.name(), query);
            } else {
                kept.add(new SqlWith.Cte(c.name(), query, c.materialized()));
            }
        }
        if (inPlace.isEmpty()) {
            return w;
        }
        SqlQuery body = substituted(w.body(), inPlace);
        return kept.isEmpty() ? body : w.with(kept, body);
    }

    /** {@code q} with each read of an expression in {@code inPlace} its query, a derived table under the read's
     *  alias, the copy's own aliases prefixed. */
    private SqlQuery substituted(SqlQuery q, Map<String, SqlQuery> inPlace) {
        if (inPlace.isEmpty()) {
            return q;
        }
        return new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Cte c && inPlace.containsKey(c.name())) {
                    SqlQuery copy = AliasPrefix.apply("s" + copies[0]++ + "_", inPlace.get(c.name()));
                    return new SqlSource.Subselect(copy, c.alias(), null);
                }
                return s;
            }
        }.rewrite(q);
    }

    /** Whether {@code q} binds a value when it is written: a plan parameter, or a raw grid's column-names report. */
    private static boolean binds(SqlQuery q) {
        boolean[] found = {false};
        new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                found[0] |= e instanceof SqlExpr.PlanParam;
                return e;
            }

            @Override
            protected SqlSource source(SqlSource s) {
                found[0] |= s instanceof SqlSource.GridReport g && g.part() == SqlSource.GridReport.Part.COLUMNS;
                return s;
            }
        }.rewrite(q);
        return found[0];
    }
}
