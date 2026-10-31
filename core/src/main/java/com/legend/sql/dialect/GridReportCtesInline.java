// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * H2's pass for a raw grid's column names inside a WITH (the planner-driver seam program's S3c, 2026-10-30): a common
 * table expression whose query reads a {@link SqlSource.GridReport.Part#COLUMNS} report binds a placeholder once per
 * reported column (its column's name, a value: bound, never written), and H2 loses a placeholder's value inside a
 * common table expression that another one reads -- an H2 defect, measured on 2.1.214 and 2.2.224, in every shape tried
 * (a select, a derived table, VALUES, a FROM; {@code H2LostCteParameterTest} pins it, so the day H2 answers 1 this pass
 * goes): {@code WITH a AS (SELECT CAST(? AS VARCHAR) AS c), s AS (SELECT COUNT(a.c) AS n FROM a) SELECT n FROM s}
 * answers 0 with the parameter set; a derived table answers 1. Each such expression is written in place, as a derived
 * table, at every reference to it, in every WITH of the statement whatever its depth. Only those: a column-names report
 * is a function of the grid's reported columns alone, so writing it twice reads the same rows twice; a report of the
 * grid's cells or rows binds nothing and stays where it is, as does every other expression.
 */
final class GridReportCtesInline extends SqlRewriter {

    /** Every WITH, at any depth (a statement's own, a subquery's), after its parts are rewritten. */
    @Override
    protected SqlQuery with(SqlWith w) {
        return inline(w);
    }

    private static SqlQuery inline(SqlQuery q) {
        if (!(q instanceof SqlWith w)) {
            return q;
        }
        Map<String, SqlQuery> inlined = new HashMap<>();
        List<SqlWith.Cte> kept = new ArrayList<>();
        for (SqlWith.Cte c : w.ctes()) {
            SqlQuery query = substituted(c.query(), inlined);
            if (readsColumnNames(query)) {
                inlined.put(c.name(), query);
            } else {
                kept.add(new SqlWith.Cte(c.name(), query, c.materialized()));
            }
        }
        if (inlined.isEmpty()) {
            return q;
        }
        SqlQuery body = substituted(w.body(), inlined);
        return kept.isEmpty() ? body : w.with(kept, body);
    }

    /** {@code q} with each reference to an inlined expression its query, as a derived table under the reference's
     *  alias (a WITH's own names are not a store's: the composition that wrote it names no table so). */
    private static SqlQuery substituted(SqlQuery q, Map<String, SqlQuery> inlined) {
        if (inlined.isEmpty()) {
            return q;
        }
        return new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                // a reference is a CTE source, or a table of the expression's name (the verdict composition's,
                // VerdictSql.cte)
                if (s instanceof SqlSource.Cte c && inlined.containsKey(c.name())) {
                    return new SqlSource.Subselect(inlined.get(c.name()), c.alias(), null);
                }
                if (s instanceof SqlSource.Table t && !t.call() && inlined.containsKey(t.name())) {
                    return new SqlSource.Subselect(inlined.get(t.name()), t.alias() == null ? t.name() : t.alias(),
                            null);
                }
                return s;
            }
        }.rewrite(q);
    }

    private static boolean readsColumnNames(SqlQuery q) {
        boolean[] found = {false};
        new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                found[0] |= s instanceof SqlSource.GridReport g && g.part() == SqlSource.GridReport.Part.COLUMNS;
                return s;
            }
        }.rewrite(q);
        return found[0];
    }
}
