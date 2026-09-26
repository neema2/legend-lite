// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * Numeric-collection LITERAL helpers shared by the arithmetic rules
 * (extracted from {@code Scalars} at the shape limit, 2026-08-20 —
 * host-logic-audit slice).
 */
final class Numerics {

    private Numerics() {
    }

    /** A LITERAL numeric list containing a DECIMAL folds to a BINARY op
     * chain — DuckDB's list aggregates and LIST_REDUCE run DOUBLE
     * (probed 2026-08-20) while binary decimal arithmetic is exact;
     * null = not that shape (the aggregate path continues). */
    static @com.legend.base.Nullable SqlExpr decimalChain(SqlExpr list,
            SqlFn op) {
        return decimalChain(list, op, java.util.function.UnaryOperator.identity());
    }

    /** {@code widen} rewrites each operand as it joins the chain (the
     *  near-INT64-edge literal widening — an all-integer literal run folds
     *  here first). */
    static @com.legend.base.Nullable SqlExpr decimalChain(SqlExpr list,
            SqlFn op, java.util.function.UnaryOperator<SqlExpr> widen) {
        // fires for DECIMAL-bearing literal lists (exact binary decimal
        // arithmetic vs LIST_PRODUCT's DOUBLE degradation) AND — Part-1
        // fix 2026-08-26 — for ALL-INTEGER literal lists (LIST_PRODUCT
        // re-kinded [2,3]->times() to DOUBLE 6.0; a TIMES chain keeps
        // the integer kind with zero new carrier sites). Non-literal
        // integer collections keep LIST_PRODUCT (no demanded traffic;
        // the wire census flags any kind drift it produces).
        if (list instanceof SqlExpr.ArrayLit la
                && la.elements().size() >= 2
                && (la.elements().stream()
                        .anyMatch(e -> e instanceof SqlExpr.DecimalLit)
                        || la.elements().stream().allMatch(e ->
                                e instanceof SqlExpr.IntLit))
                && la.elements().stream().allMatch(e ->
                        e instanceof SqlExpr.DecimalLit
                        || e instanceof SqlExpr.IntLit
                        || e instanceof SqlExpr.FloatLit)) {
            SqlExpr acc = widen.apply(la.elements().get(0));
            for (int i = 1; i < la.elements().size(); i++) {
                acc = SqlExpr.Call.of(op, acc, widen.apply(la.elements().get(i)));
            }
            return acc;
        }
        return null;
    }

    /** A LITERAL list of {@code [1]}-scalar OPERANDS under plus/times —
     * the parser's infix desugar and the explicit {@code times([$a, 2,
     * $b, 100])} spelling alike — renders as the engine's binary chain
     * ({@code a * b * c}: pureToSQLQuery's plus/times dyna-function over N
     * arguments; NULL-propagating — a NULL operand nulls the product,
     * witness testFilterTimesWithManyOperands' 'no Firm' row), never the
     * list aggregate (which SKIPS NULL elements). Real collections (a
     * to-many read, a non-literal list) keep the aggregate. Null = not
     * that shape. */
    static @com.legend.base.Nullable SqlExpr scalarChain(
            com.legend.compiler.spec.typed.TypedSpec typedArg, SqlExpr list,
            SqlFn op) {
        return scalarChain(typedArg, list, op, java.util.function.UnaryOperator.identity());
    }

    /** {@code widen} rewrites each operand before it joins the chain (the
     *  near-INT64-edge literal widening). */
    static @com.legend.base.Nullable SqlExpr scalarChain(
            com.legend.compiler.spec.typed.TypedSpec typedArg, SqlExpr list,
            SqlFn op, java.util.function.UnaryOperator<SqlExpr> widen) {
        if (!(typedArg instanceof com.legend.compiler.spec.typed.TypedCollection tc)
                || tc.elements().size() < 2
                || !(list instanceof SqlExpr.ArrayLit la)
                || la.elements().size() != tc.elements().size()) {
            return null;
        }
        // an OPERATOR RUN (a + b, the parser's infix carrier) that reached the
        // rule as a plain literal — LambdaBinding lowers it so only when every
        // operand is SQL-lane (StoreLane) — IS the operator chain: SQL
        // arithmetic promotes across kinds exactly as the engine's does (Float
        // column - Integer literal); the one-kind rule below is the VALUE
        // list's (a mixed literal rides the variant carrier)
        if (tc.operatorRun()) {
            SqlExpr acc = widen.apply(la.elements().get(0));
            for (int i = 1; i < la.elements().size(); i++) {
                acc = SqlExpr.Call.of(op, acc, widen.apply(la.elements().get(i)));
            }
            return acc;
        }
        // ONE primitive numeric kind across the operands: a NUMBER-LUB
        // mixed literal ([1, 2.5]) rides the variant carrier (JSON cells —
        // '+(JSON, JSON)' is a Binder error; grammar witnesses
        // testPlusNumber / testDecimalPlus) and keeps the aggregate path
        // (numList unwraps it)
        com.legend.compiler.element.type.Type kind = null;
        for (com.legend.compiler.spec.typed.TypedSpec e : tc.elements()) {
            if (!(e.info().multiplicity()
                    instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                    && Integer.valueOf(1).equals(b.upper()))) {
                return null;
            }
            com.legend.compiler.element.type.Type t = e.info().type();
            if (!(t == com.legend.compiler.element.type.Type.Primitive.INTEGER
                    || t == com.legend.compiler.element.type.Type.Primitive.FLOAT
                    || t == com.legend.compiler.element.type.Type.Primitive.DECIMAL)
                    || (kind != null && kind != t)) {
                return null;
            }
            kind = t;
        }
        SqlExpr acc = widen.apply(la.elements().get(0));
        for (int i = 1; i < la.elements().size(); i++) {
            acc = SqlExpr.Call.of(op, acc, widen.apply(la.elements().get(i)));
        }
        return acc;
    }

    /** Real Compare.java's KIND ordering: Numbers < Dates < Booleans <
     * Strings; -1 = not a primitive kind (moved from Scalars at the
     * shape limit — the cross-kind comparison vocabulary). */
    static int compareKind(com.legend.compiler.element.type.Type t) {
        if (t == com.legend.compiler.element.type.Type.Primitive.INTEGER
                || t == com.legend.compiler.element.type.Type.Primitive.FLOAT
                || t == com.legend.compiler.element.type.Type.Primitive.NUMBER
                || t == com.legend.compiler.element.type.Type.Primitive.DECIMAL
                || t instanceof com.legend.compiler.element.type.Type.PrecisionDecimal) {
            return 0;
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE
                || t == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE
                || t == com.legend.compiler.element.type.Type.Primitive.DATE_TIME) {
            return 1;
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.BOOLEAN) {
            return 2;
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.STRING) {
            return 3;
        }
        return -1;
    }

    /** The mixed-number VARIANT carrier's literal array unwraps to raw
     * numerics for aggregates/reductions (sum(JSON) does not bind). */
    static SqlExpr numList(SqlExpr e) {
        if (e instanceof SqlExpr.ArrayLit la && !la.elements().isEmpty()
                && la.elements().stream().allMatch(
                        MixedEncoding::variantWrapped)) {
            return new SqlExpr.ArrayLit(la.elements().stream()
                    .map(MixedEncoding::unwrapVariant)
                    .toList());
        }
        return e;
    }
}
