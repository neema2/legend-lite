// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;

/**
 * LEGEND-PURE'S TEXT OF A LIST, {@code [a, b]} (B8, 2026-11-17): the edge's form of the plan style's two-form kind
 * {@code lowering.ListTexts} -- composed in SQL as the lowering wrote it beside the product's form until then (the
 * second operand of which the edge's spelling pass kept), so the SQL is byte-identical.
 */
final class PureListTexts implements com.legend.lowering.ListTexts {

    static final PureListTexts INSTANCE = new PureListTexts();

    private PureListTexts() {
    }

    /** The JSON array's elements as root text, joined {@code ', '} in brackets. */
    @Override
    public SqlExpr anyList(SqlExpr x) {
        RelationTextArm.valuePrint("anyList");   // SPIKE (H11): the value-print census
        return bracketed(new SqlExpr.Cast(x, new com.legend.sql.SqlType.Array(com.legend.sql.SqlType.Scalar.VARCHAR),
                SqlExpr.Reason.CARRIER));
    }

    /** Each element printed as its kind (legend-pure's {@code toString} under Any), joined {@code ', '} in brackets. */
    @Override
    public SqlExpr mixedList(com.legend.lowering.Styled styled, SqlExpr.TaggedList tl) {
        RelationTextArm.valuePrint("mixedList");   // SPIKE (H11): the value-print census
        String p = "_tlp";
        return bracketed(SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, tl.positions(), new SqlExpr.Lambda(List.of(p),
                com.legend.lowering.RelationTexts.pureToString(styled,
                        new com.legend.compiler.element.type.Type.ClassType(
                                com.legend.compiler.element.type.PlatformTypes.ANY),
                        tl.element(SqlExpr.Column.derived(null, p))))));
    }

    private static SqlExpr bracketed(SqlExpr texts) {
        SqlExpr joined = SqlExpr.Call.of(SqlFn.COALESCE,
                new SqlExpr.ReduceCollection(SqlAgg.Fn.STRING_AGG, texts, List.of(new SqlExpr.StringLit(", "))),
                new SqlExpr.StringLit(""));
        return SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("["), joined),
                new SqlExpr.StringLit("]"));
    }
}
