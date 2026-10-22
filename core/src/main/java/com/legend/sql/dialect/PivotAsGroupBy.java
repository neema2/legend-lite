// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.List;

/**
 * A static pivot as a grouped CASE aggregation: every column of the pivot's output that is not a pivoted one is a
 * group key, and each value and aggregate {@code AGG(CASE WHEN on = v THEN arg END) AS "v__|__alias"} -- the
 * portable form CarrierStrategies writes for a database without lists, here for one with lists and no PIVOT (Trino,
 * 2026-10-12), where that pass stands aside.
 */
final class PivotAsGroupBy extends com.legend.sql.SqlRewriter {
    @Override
    protected SqlSource source(SqlSource s) {
        // a dynamic pivot a cast names pivots on the named values (SqlSource.Pivot.values, step 9, 2026-10-19)
        if (!(s instanceof SqlSource.Pivot p) || p.values().isEmpty() || p.on().size() != 1) {
            return s;
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<SqlExpr> group = new ArrayList<>();
        for (com.legend.sql.OutputCol oc : p.outputs()) {
            if (!oc.name().contains("__|__")) {
                SqlExpr g = SqlExpr.Column.of(p.source().alias(), oc);
                group.add(g);
                ps.add(new SqlSelect.Projection(g, oc.name(), oc));
            }
        }
        for (SqlExpr v : p.values()) {
            for (SqlSource.Pivot.Using u : p.usings()) {
                SqlAgg.Reducer agg = u.agg();
                List<SqlExpr> args = new ArrayList<>(agg.args());
                if (args.isEmpty()) {
                    throw new DialectCapability("a pivot whose aggregate takes no argument reached a dialect without a"
                            + " native pivot");
                }
                args.set(0, new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.EQUAL, p.on().get(0), v), args.get(0))), null));
                String name = pivotValueText(v) + "__|__" + u.alias();
                ps.add(new SqlSelect.Projection(new SqlAgg.Reducer(agg.fn(), args, agg.distinct(), agg.orderBy()),
                        name, CarrierStrategies.generatedSlot(p, u, name)));
            }
        }
        return new SqlSource.Subselect(SqlSelect.starOf(p.source()).withProjections(ps).withGroupBy(group),
                p.alias(), null);
    }

    /** A pivot value as the platform names its column. */
    private static String pivotValueText(SqlExpr v) {
        return switch (v) {
            case SqlExpr.StringLit s -> s.value();
            case SqlExpr.IntLit i -> String.valueOf(i.value());
            case SqlExpr.BoolLit b -> String.valueOf(b.value());
            default -> throw new DialectCapability("a pivot value " + v.getClass().getSimpleName()
                    + " reached a dialect without a native pivot, with no column name");
        };
    }
}
