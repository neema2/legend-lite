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

    /**
     * The pure {@code [0..1]} comparison overloads' bodies, inlined at the
     * COMPARISON SITE (audit 20a H2 — real mechanism, not a not()-side
     * wrap): {@code greaterThan(left:Number[0..1], right:Number[1]) =
     * $left->isNotEmpty() && greaterThan($left->toOne(), $right)}
     * (legend-pure inequality/greaterThan.pure; engine stringExtension.pure
     * for startsWith/endsWith). Each OPTIONAL non-literal operand
     * contributes an {@code IS NOT NULL} conjunct — active in EVERY
     * context (negated, value position, composed), exactly like the
     * inlined overload body reaching SQL as
     * {@code X is not null and X > 30} (engine testFilters golden).
     */
    static SqlExpr optionalOperandGuards(
            com.legend.compiler.spec.typed.TypedNativeCall n,
            List<SqlExpr> loweredArgs, SqlExpr cmp) {
        List<SqlExpr> conj = new java.util.ArrayList<>();
        for (int i = 0; i < loweredArgs.size(); i++) {
            if (isOptional(n.args().get(i).info().multiplicity())
                    && !isSqlLiteral(loweredArgs.get(i))) {
                conj.add(SqlExpr.Call.of(SqlFn.IS_NOT_NULL, loweredArgs.get(i)));
            }
        }
        if (conj.isEmpty()) {
            return cmp;
        }
        conj.add(cmp);
        return new SqlExpr.Call(SqlFn.AND, conj);
    }

    private static boolean isOptional(
            com.legend.compiler.element.type.Multiplicity m) {
        return m instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.lower() == 0 && Integer.valueOf(1).equals(b.upper());
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
                || e instanceof SqlExpr.TimestampLit
                || e instanceof SqlExpr.NullLit;
    }

}
