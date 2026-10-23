// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

/**
 * An EXISTS that one row decides, without its subquery:
 * <ul>
 *   <li>over a source of one row (a one-row VALUES, or none) whose condition reads no column of it -- the IQ island's
 *       {@code vc IN (SELECT 'x' FROM DUMMY)} -- it is its condition, false where unknown: {@code COALESCE(p,
 *       FALSE)};</li>
 *   <li>over a select that aggregates with no grouping, or a derived table that does and a select that filters
 *       nothing, it is TRUE: such a select has one row, over no rows too.</li>
 * </ul>
 * Snowflake evaluates neither as written: the first correlates through no column of its own, the second two levels
 * down ("Unsupported subquery type cannot be evaluated", the IQ scale program's Snowflake lane, 2026-10-23).
 */
final class ExistsOverOneRow extends SqlRewriter {

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (!(e instanceof SqlExpr.Exists x) || !(x.subquery() instanceof SqlSelect s) || !keepsItsRows(s)) {
            return e;
        }
        if (oneRowAggregate(s)) {
            // EXISTS (SELECT COUNT(*) ... WHERE p): one row, whatever p
            return new SqlExpr.BoolLit(true);
        }
        SqlSource from = s.from();
        boolean oneRow = from instanceof SqlSource.Dual
                || from instanceof SqlSource.Values v && v.rows().size() == 1 && !reads(s.where(), v.alias());
        if (oneRow) {
            return s.where() == null ? new SqlExpr.BoolLit(true)
                    : SqlExpr.Call.of(SqlFn.COALESCE, s.where(), new SqlExpr.BoolLit(false));
        }
        if ((s.where() == null || s.where() instanceof SqlExpr.BoolLit b && b.value())
                && from instanceof SqlSource.Subselect sub && sub.inner() instanceof SqlSelect in
                && oneRowAggregate(in)) {
            return new SqlExpr.BoolLit(true);
        }
        return e;
    }

    /** No clause of {@code s} drops or groups its source's rows past its WHERE. */
    private static boolean keepsItsRows(SqlSelect s) {
        return s.groupBy().isEmpty() && s.having() == null && s.qualify() == null && s.offset() == null
                && (s.limit() == null || s.limit() >= 1);
    }

    /** {@code in} aggregates with no grouping, and nothing drops its one row. */
    private static boolean oneRowAggregate(SqlSelect in) {
        return keepsItsRows(in) && in.projections().stream().anyMatch(p -> SqlAgg.aggregatesSelect(p.expr()));
    }

    /** Whether {@code e} reads a column of {@code alias}, its subqueries' included. */
    private static boolean reads(@com.legend.base.Nullable SqlExpr e, String alias) {
        if (e == null) {
            return false;
        }
        boolean[] found = {false};
        new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr c) {
                found[0] |= c instanceof SqlExpr.Column col && (col.table() == null || col.table().equals(alias));
                return c;
            }

            SqlExpr run(SqlExpr root) {
                return rewriteExpr(root);
            }
        }.run(e);
        return found[0];
    }
}
