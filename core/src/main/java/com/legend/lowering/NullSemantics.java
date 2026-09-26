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
        // the engine carries guard+comparison as a GROUP (its
        // moveExtraFilterToFilter wrap): standalone it renders
        // parenthesized; merged into a larger and-chain the group
        // flattens away (mergeAnd)
        return new SqlExpr.Group(new SqlExpr.Call(SqlFn.AND, conj));
    }

    private static boolean isOptional(
            com.legend.compiler.element.type.Multiplicity m) {
        return m instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.lower() == 0 && Integer.valueOf(1).equals(b.upper());
    }

    /** Engine processNotEqual (dbExtension.pure:970-976): two LITERAL
     * operands stay a bare {@code <>}; every other {@code not(equal)}
     * redirects to the SEMANTIC {@code nullSafeNotEqual} node, which the
     * DIALECT spells ({@code is distinct from} on H2/DuckDB, the
     * OR-expansion on defaults-derived dialects). */
    static SqlExpr notEqualNullArms(java.util.List<SqlExpr> ops) {
        SqlExpr left = ops.get(0);
        SqlExpr right = ops.get(1);
        // a NULL-literal operand (sqlNull()/TDSNull — the null-cell value):
        // pure {@code x != TDSNull} IS the presence test — the generic
        // arms would emit {@code x <> NULL OR x IS NULL}, which keeps
        // ONLY nulls (inverted; tds.pure firstNotNull golden)
        if (left instanceof SqlExpr.NullLit || right instanceof SqlExpr.NullLit) {
            SqlExpr other = left instanceof SqlExpr.NullLit ? right : left;
            return other instanceof SqlExpr.NullLit
                    ? new SqlExpr.BoolLit(false)
                    : SqlExpr.Call.of(SqlFn.IS_NOT_NULL, other);
        }
        if (isSqlLiteral(left) && isSqlLiteral(right)) {
            return new SqlExpr.Call(SqlFn.NOT_EQUAL, ops);
        }
        return new SqlExpr.Call(SqlFn.NULL_SAFE_NOT_EQUAL, ops);
    }

    /** Engine isEqualsFromFilter (dbExtension.pure:926-930): equality
     * of two NULLABLE COLUMNS is NULL-SAFE — pure empty==empty is TRUE
     * ({@code a = b OR (a is null AND b is null)}, the
     * testConsistencyWithNulls col-to-col golden). Approximated by
     * operand shape (both plain columns, both optional operands) — the
     * engine's callingFromFilter flag has no analog at this layer; the
     * corpus goldens referee. Anything else stays the bare EQUAL. */
    /** The VERBATIM-EQUALITY rewrite (Phase 2a, batch 136): a resolver-
     * synthesized join condition or a CORRELATION-stamped filter is the
     * MAPPING's own definition and lowers plain '=' — the null-safe grant
     * is for USER pure equality only (TypedJoin.userCondition;
     * slotDemandJoins' golden pins the distinction). The mode is the
     * Lowerer's own state, applied to what the equality rule emitted;
     * until batch 136 it was a thread-local the rule read. */
    static SqlExpr verbatim(boolean verbatim, SqlExpr e) {
        return verbatim && e instanceof SqlExpr.Call c && c.fn() == SqlFn.NULL_SAFE_EQUAL
                ? new SqlExpr.Call(SqlFn.EQUAL, c.args())
                : e;
    }

    static SqlExpr equalNullArms(
            com.legend.compiler.spec.typed.TypedNativeCall n,
            java.util.List<SqlExpr> ops) {
        // POSITION-BLIND (engine nullSafeEqualsOperation case 5: both
        // lower bounds 0 → nullSafeEqual, no position gate — witness
        // testProjectEqualityOnNullableColumns, where the equality sits
        // in a PROJECT column); the operands' own multiplicities decide.
        // A verbatim context (the mapping's own definition) rewrites the
        // result to plain '=' at the Lowerer ({@link #verbatim}).
        if (ops.size() == 2
                && ops.get(0) instanceof SqlExpr.Column
                && ops.get(1) instanceof SqlExpr.Column
                && isOptional(n.args().get(0).info().multiplicity())
                && isOptional(n.args().get(1).info().multiplicity())) {
            return new SqlExpr.Call(SqlFn.NULL_SAFE_EQUAL, ops);
        }
        return new SqlExpr.Call(SqlFn.EQUAL, ops);
    }

    /** Engine pureToSQLQuery: an enum-referencing comparison routes the
     *  ENUM machinery (processEqualsForEnum / equalEnumOpSelector) and
     *  NEVER becomes nullSafeEqual — the hasReferenceToEnum branch runs
     *  BEFORE nullSafeEqualsOperation. */
    static boolean enumInvolved(
            com.legend.compiler.spec.typed.TypedSpec inner) {
        if (inner instanceof com.legend.compiler.spec.typed.TypedNativeCall c) {
            for (var a : c.args()) {
                if (a.info().type()
                        instanceof com.legend.compiler.element.type.Type.EnumType) {
                    return true;
                }
            }
        }
        return false;
    }

    /** The PRE-node OR-expansion, kept ONLY for enum-referencing
     *  comparisons: the engine's enum machinery
     *  (equalEnumOperationSelector) recognizes exactly this shape —
     *  {@code <>} plus explicit null arms — and the plan-text templates
     *  fold it; a semantic null-safe node would break the selector. */
    private static SqlExpr notEqualExpandedArms(java.util.List<SqlExpr> ops) {
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

    /** Engine processNot's SPECIAL arms — {@code not(equal)} redirects
     *  through {@link #notEqualNullArms}, {@code not(nullSafeEqual)}
     *  flips to {@code nullSafeNotEqual} (and back), and
     *  {@code not(in)} gains the unconditional {@code OR L IS NULL}
     *  (dbExtension.pure processNotIn); null = no special arm, the
     *  caller emits the BARE not. */
    static @com.legend.base.Nullable SqlExpr negate(SqlExpr inner,
            boolean enumInvolved) {
        if (!(inner instanceof SqlExpr.Call c)) {
            return null;
        }
        return switch (c.fn()) {
            case EQUAL -> enumInvolved
                    ? notEqualExpandedArms(c.args())
                    : notEqualNullArms(c.args());
            case NULL_SAFE_EQUAL ->
                    new SqlExpr.Call(SqlFn.NULL_SAFE_NOT_EQUAL, c.args());
            case NULL_SAFE_NOT_EQUAL ->
                    new SqlExpr.Call(SqlFn.NULL_SAFE_EQUAL, c.args());
            case IN -> new SqlExpr.Call(SqlFn.OR, List.of(
                    new SqlExpr.Call(SqlFn.NOT, List.of(inner)),
                    SqlExpr.Call.of(SqlFn.IS_NULL, c.args().get(0))));
            default -> null;
        };
    }

    private static boolean isSqlLiteral(SqlExpr e) {
        return e instanceof SqlExpr.StringLit || e instanceof SqlExpr.IntLit
                || e instanceof SqlExpr.FloatLit || e instanceof SqlExpr.DecimalLit
                || e instanceof SqlExpr.BoolLit || e instanceof SqlExpr.DateLit
                || e instanceof SqlExpr.TimestampLit
                || e instanceof SqlExpr.NullLit;
    }

}
