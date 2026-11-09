// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.error.DialectCapability;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.List;

/**
 * An as-of join for a dialect with none of its own (Trino, 2026-10-12): {@code s}, whose FROM is an as-of join, as a
 * LEFT JOIN on the same condition, each left row keeping its matches whose right key is the extreme one --
 * {@code QUALIFY k = MAX(k) OVER (PARTITION BY every left column) OR MAX(k) OVER (...) IS NULL}, the second arm keeping
 * a left row with no match -- a QUALIFY, which QualifyToSubselect writes as a filter over a wrapping select where there
 * is none (Databricks' form; the portable form's correlated MAX in the join condition is refused by Trino, measured).
 * Only under a select that filters, groups and windows nothing of its own.
 */
final class AsOfAsPick extends com.legend.sql.SqlRewriter {

    @Override
    protected com.legend.sql.SqlQuery select(SqlSelect s) {
        if (!(s.from() instanceof SqlSource.Join j) || j.kind() != SqlSource.Join.Kind.ASOF_LEFT) {
            return s;
        }
        CarrierStrategies.AsOfKey key = CarrierStrategies.asOfKey(j);
        if (key == null) {
            throw new DialectCapability("an as-of join with no inequality between its sides reached a dialect"
                    + " without an as-of join");
        }
        if (s.where() != null || !s.groupBy().isEmpty() || s.having() != null || s.qualify() != null
                || s.projections().stream().anyMatch(p -> windowed(p.expr()))) {
            throw new DialectCapability("an as-of join under a select that filters, groups or windows reached a"
                    + " dialect without an as-of join: its pick is that select's filter");
        }
        String left = j.left().alias();
        if (left == null || j.left().outputs().isEmpty()) {
            throw new DialectCapability("an as-of join whose left side has no alias and columns reached a dialect"
                    + " without an as-of join");
        }
        List<SqlExpr> partition = new ArrayList<>();
        for (com.legend.sql.OutputCol oc : j.left().outputs()) {
            partition.add(SqlExpr.Column.of(left, oc));
        }
        SqlExpr extreme = new SqlExpr.WindowCall(new SqlAgg.Reducer(key.pickMax() ? SqlAgg.Fn.MAX : SqlAgg.Fn.MIN,
                List.of(key.rightKey()), false, List.of()), partition, List.of(), null);
        SqlExpr pick = SqlExpr.Call.of(SqlFn.OR, SqlExpr.Call.of(SqlFn.EQUAL, key.rightKey(), extreme),
                SqlExpr.Call.of(SqlFn.IS_NULL, extreme));
        return s.withFrom(new SqlSource.Join(j.left(), j.right(), SqlSource.Join.Kind.LEFT, j.on()))
                .withQualify(pick);
    }

    private static boolean windowed(SqlExpr e) {
        if (e instanceof SqlExpr.WindowCall) {
            return true;
        }
        for (SqlExpr k : e.children()) {
            if (windowed(k)) {
                return true;
            }
        }
        return false;
    }
}
