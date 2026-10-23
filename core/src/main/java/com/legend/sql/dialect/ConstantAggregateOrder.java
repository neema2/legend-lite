// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;

import java.util.List;

/**
 * A string or list aggregate's ORDER BY key that is a constant orders nothing, and is dropped: DuckDB refuses a
 * non-integer one ("ORDER BY non-integer literal has no effect"), and reads none as the value. The IQ scale program's
 * fuzz round 8 (2026-10-23): {@code LIST(x0.c2, ',' ORDER BY x0.c2)} over a derived table's {@code UCASE('')} folded
 * the key to {@code ''}. Only these two: a percentile's key is the value it aggregates ({@link QuantileOrder}).
 */
final class ConstantAggregateOrder extends SqlRewriter {

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (!(e instanceof SqlAgg.Reducer r) || r.fn() != SqlAgg.Fn.STRING_AGG && r.fn() != SqlAgg.Fn.LIST
                || r.orderBy().stream().noneMatch(k -> constant(k.expr()))) {
            return e;
        }
        List<SqlSelect.SortKey> kept = r.orderBy().stream().filter(k -> !constant(k.expr())).toList();
        return new SqlAgg.Reducer(r.fn(), r.args(), r.distinct(), kept, r.type());
    }

    private static boolean constant(SqlExpr e) {
        if (e instanceof SqlExpr.Group g) {
            return constant(g.inner());
        }
        return e instanceof SqlExpr.StringLit || e instanceof SqlExpr.IntLit || e instanceof SqlExpr.FloatLit
                || e instanceof SqlExpr.DecimalLit || e instanceof SqlExpr.BoolLit || e instanceof SqlExpr.NullLit
                || e instanceof SqlExpr.DateLit;
    }
}
