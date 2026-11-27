// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;

import java.util.ArrayList;
import java.util.List;

/**
 * The PORTABLE form of {@link SqlFn#SAME_IN_ORDER} and {@link SqlFn#SAME_IN_ANY_ORDER} (item 3's S4, 2026-11-22, the
 * user: "Yes on list compare"): two sides' typed rows, each its values -- one column ({@code __o}), or a grid row's
 * cells, one column each -- and {@code __rn}, its place in its side's order, the same rows, as one Boolean select every
 * database runs: in order, the same count and every place's two rows alike, column by column {@code IS NOT DISTINCT
 * FROM}; in any order, no row whose signed count differs (one signed {@code UNION ALL} grouped by every value column).
 * The two sides' value columns pair by position. It needs no list equality, no {@code EXCEPT ALL} and no
 * {@code FULL OUTER JOIN} (H2 has none: M1 of the assert-records homework). A dialect whose database compares lists itself writes its own form instead; this is
 * the base renderer's, a decomposition the dialect owns (AGENTS.md invariant 3), never the lowering's.
 */
final class SameValuesForm {
    private static final String O = "__o";
    private static final String RN = "__rn";
    private static final String K = "__k";
    private static final String SAME = "__same";

    private SameValuesForm() {
    }

    /** The Boolean select comparing {@code eRows} and {@code aRows}. A value column holding a list or a map is refused
     *  by name: this form compares scalar columns, and a database without the collection type carries one as its JSON
     *  text, whose comparison would be of texts, not values (item 3's S4, 2026-11-22: typed values only). */
    static SqlQuery of(SqlQuery eRows, SqlQuery aRows, boolean ordered) {
        for (OutputCol c : values(eRows)) {
            if (c.type() instanceof SqlType.Array || c.type() instanceof SqlType.Map) {
                // (a query's comparison of two whole sides is refused by the same words since the edition program's
                // step 6: the product's equality over two sides is the judge's)
                throw new com.legend.sql.DialectCapability("a comparison of lists or maps of values (a query's ==, an"
                        + " assert's) has no typed form on a database that carries them as JSON: refused");
            }
        }
        return ordered ? inOrder(eRows, aRows) : anyOrder(eRows, aRows);
    }

    private static SqlQuery inOrder(SqlQuery eRows, SqlQuery aRows) {
        SqlQuery ce = counted(eRows);
        SqlQuery ca = counted(aRows);
        List<OutputCol> ev = values(eRows);
        List<OutputCol> av = values(aRows);
        SqlExpr on = SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.of("x", eRows.outputs(), RN),
                SqlExpr.Column.of("y", aRows.outputs(), RN));
        for (int i = 0; i < ev.size(); i++) {
            on = SqlExpr.Call.of(SqlFn.AND, on, SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                    SqlExpr.Column.of("x", eRows.outputs(), ev.get(i).name()),
                    SqlExpr.Column.of("y", aRows.outputs(), av.get(i).name())));
        }
        SqlSource pairs = new SqlSource.Join(new SqlSource.Subselect(eRows, "x", null),
                new SqlSource.Subselect(aRows, "y", null), SqlSource.Join.Kind.INNER, on);
        OutputCol kOut = new OutputCol(K, SqlType.Scalar.BIGINT, false);
        SqlSelect cm = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                List.of(SqlExpr.Column.of("x", eRows.outputs(), RN)), false, List.of()), K, kOut)), false, pairs, null,
                List.of(), null, null, List.of(), null, null, List.of(kOut));
        SqlExpr kE = SqlExpr.Column.of("ce", ce.outputs(), K);
        SqlExpr same = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, kE, SqlExpr.Column.of("ca", ca.outputs(), K)),
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.of("cm", cm.outputs(), K), kE));
        OutputCol sOut = new OutputCol(SAME, SqlType.Scalar.BOOLEAN, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(same, SAME, sOut)), false,
                cross(cross(new SqlSource.Subselect(ce, "ce", null), new SqlSource.Subselect(ca, "ca", null)),
                        new SqlSource.Subselect(cm, "cm", null)),
                null, List.of(), null, null, List.of(), null, null, List.of(sOut));
    }

    private static SqlQuery anyOrder(SqlQuery eRows, SqlQuery aRows) {
        List<OutputCol> ev = values(eRows);
        List<OutputCol> uOuts = new ArrayList<>();
        for (int i = 0; i < ev.size(); i++) {
            uOuts.add(new OutputCol(ev.size() == 1 ? O : O + i, ev.get(i).type(), true));
        }
        uOuts.add(new OutputCol("__g", SqlType.Scalar.BIGINT, false));
        SqlUnion u = new SqlUnion(List.of(signed(eRows, 1L, uOuts), signed(aRows, -1L, uOuts)), true, uOuts);
        List<SqlExpr> by = new ArrayList<>();
        for (int i = 0; i < ev.size(); i++) {
            by.add(SqlExpr.Column.of("u", uOuts, uOuts.get(i).name()));
        }
        SqlSelect differing = new SqlSelect(List.of(new SqlSelect.Projection(by.get(0), uOuts.get(0).name(),
                uOuts.get(0))), false,
                new SqlSource.Subselect(u, "u", null), null, by,
                SqlExpr.Call.of(SqlFn.NOT_EQUAL, new SqlAgg.Reducer(SqlAgg.Fn.SUM,
                        List.of(SqlExpr.Column.of("u", uOuts, "__g")), false, List.of()), new SqlExpr.IntLit(0)),
                null, List.of(), null, null, List.of(uOuts.get(0)));
        OutputCol kOut = new OutputCol(K, SqlType.Scalar.BIGINT, false);
        SqlSelect d = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(),
                false, List.of()), K, kOut)), false, new SqlSource.Subselect(differing, "h", null), null, List.of(),
                null, null, List.of(), null, null, List.of(kOut));
        OutputCol sOut = new OutputCol(SAME, SqlType.Scalar.BOOLEAN, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.EQUAL,
                SqlExpr.Column.of("d", d.outputs(), K), new SqlExpr.IntLit(0)), SAME, sOut)), false,
                new SqlSource.Subselect(d, "d", null), null, List.of(), null, null, List.of(), null, null,
                List.of(sOut));
    }

    /** A side's value columns: every column but its place ({@code __rn}), in order. */
    private static List<OutputCol> values(SqlQuery rows) {
        return rows.outputs().stream().filter(o -> !o.name().equals(RN)).toList();
    }

    /** A side's rows each with its sign: {@code +1} for the expected's, {@code -1} for the actual's. */
    private static SqlSelect signed(SqlQuery rows, long sign, List<OutputCol> outs) {
        List<OutputCol> v = values(rows);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (int i = 0; i < v.size(); i++) {
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of("s", rows.outputs(), v.get(i).name()),
                    outs.get(i).name(), outs.get(i)));
        }
        ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(sign), "__g", outs.get(v.size())));
        return new SqlSelect(ps, false, new SqlSource.Subselect(rows, "s", null), null, List.of(), null, null,
                List.of(), null, null, outs);
    }

    /** A side's count of values. */
    private static SqlQuery counted(SqlQuery rows) {
        OutputCol kOut = new OutputCol(K, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                List.of(SqlExpr.Column.of("c", rows.outputs(), RN)), false, List.of()), K, kOut)), false,
                new SqlSource.Subselect(rows, "c", null), null, List.of(), null, null, List.of(), null, null,
                List.of(kOut));
    }

    private static SqlSource cross(SqlSource l, SqlSource r) {
        return new SqlSource.Join(l, r, SqlSource.Join.Kind.CROSS, null);
    }
}
