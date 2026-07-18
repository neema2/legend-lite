// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;

/**
 * The engine's NULL-COMPENSATION emissions for negated predicates
 * (dbExtension.pure processNotEqual/processNotIn): pure equality is
 * TOTAL over collections (eq over empty is false, so {@code x != v}
 * MATCHES null x) while SQL three-valued {@code <>} silently drops null
 * rows — the corpus's null-consistency family pins the difference
 * (task #62). Split from {@link Scalars} (file-size seam).
 */
final class NullSemantics {

    private NullSemantics() {
    }

    /** Engine processNotEqual arms (dbExtension.pure), keyed on operand
     * LITERAL-ness: two literals bare; a column side gains OR IS NULL;
     * col-vs-col adds both single-null arms (both-null compares EQUAL). */
    static SqlExpr notEqualNullArms(java.util.List<SqlExpr> ops) {
        SqlExpr left = ops.get(0);
        SqlExpr right = ops.get(1);
        SqlExpr ne = new SqlExpr.Call(SqlFn.NOT_EQUAL, ops);
        boolean litL = isSqlLiteral(left);
        boolean litR = isSqlLiteral(right);
        if (litL && litR) {
            return ne;
        }
        if (litL != litR) {
            SqlExpr col = litL ? right : left;
            return new SqlExpr.Call(SqlFn.OR, List.of(ne,
                    SqlExpr.Call.of(SqlFn.IS_NULL, col)));
        }
        return new SqlExpr.Call(SqlFn.OR, List.of(ne,
                new SqlExpr.Call(SqlFn.AND, List.of(
                        SqlExpr.Call.of(SqlFn.IS_NULL, left),
                        SqlExpr.Call.of(SqlFn.IS_NOT_NULL, right))),
                new SqlExpr.Call(SqlFn.AND, List.of(
                        SqlExpr.Call.of(SqlFn.IS_NOT_NULL, left),
                        SqlExpr.Call.of(SqlFn.IS_NULL, right)))));
    }

    private static boolean isSqlLiteral(SqlExpr e) {
        return e instanceof SqlExpr.StringLit || e instanceof SqlExpr.IntLit
                || e instanceof SqlExpr.FloatLit || e instanceof SqlExpr.DecimalLit
                || e instanceof SqlExpr.BoolLit || e instanceof SqlExpr.DateLit
                || e instanceof SqlExpr.NullLit;
    }

}
