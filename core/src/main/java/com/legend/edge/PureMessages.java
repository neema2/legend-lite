// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;

/**
 * LEGEND-PURE'S SENTENCES OF A RAISE, IN SQL (phase D's D1.1, the user's ruling of 2026-11-10: "Pure's full sentence
 * with the value is spelled at the edge, for PCT and pure/v1"): the message the edge's dialects write for a raise of
 * each kind, built from its typed operands exactly as the product's lowering built it until its site moved here
 * (byte for byte: PCT's {@code assertError}s compare legend-pure's text). Each arm cites its upstream line in the D0
 * inventory's reads ({@code docs/compat-inventory-homework/upstream-reads.txt}). A kind whose sites have not moved
 * yet is refused by name: nothing raises it.
 */
public final class PureMessages {

    private PureMessages() {
    }

    /** legend-pure's message of {@code r}, as SQL the edge's dialect writes. */
    public static SqlExpr sentence(SqlExpr.RaiseText r) {
        return switch (r.kind()) {
            // the user's own message, as the product writes it
            case USER_FAIL -> r.operands().get(0);
            // rem's and quotient's, the dividend its operand: 'Cannot divide 5 by zero' (legend-pure's
            // CompiledSupport:1485); Integer / Integer's, no operand: the interpreter's BigDecimal 'Division by zero'
            // (its NumericAccumulator; the D0 inventory's 1.2)
            case DIVIDE_BY_ZERO -> r.operands().isEmpty() ? new SqlExpr.StringLit("Division by zero")
                    : cat(new SqlExpr.StringLit("Cannot divide "), str(r.operands().get(0)),
                            new SqlExpr.StringLit(" by zero"));
            case SHIFT_OUT_OF_RANGE, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, INSERT_OUT_OF_BOUNDS,
                 SQRT_OF_NEGATIVE, SIZE_NOT_MULTIPLICITY, SLICE_BOUNDS_INVERTED, DATE_COMPONENT_INVALID, CAST_IMPOSSIBLE,
                 RANGE_STEP_ZERO, CONSTRAINT_UNEVALUABLE, LIST_LENGTHS_DIFFER, MAP_KEYS_MISALIGNED, MAP_KEY_NULL,
                 MAP_KEYS_NOT_UNIQUE, TEXT_NOT_INTEGER, TEXT_NOT_DATE, VALUE_NOT_HELD, FRACTION_NOT_KEPT,
                 CSV_TOO_WIDE -> throw new UnsupportedOperationException("a raise of kind " + r.kind()
                    + " reached the edge: no site raises it yet (phase D's D1.1 moves its sites one family at a time)");
        };
    }

    /** {@code parts} as one text, in order ({@code CONCAT}, as the lowering's own {@code cat} builds it). */
    static SqlExpr cat(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT, out, parts[i]);
        }
        return out;
    }

    /** {@code x}'s text as the database casts it ({@code VARCHAR}), as the lowering's own {@code str} does. */
    static SqlExpr str(SqlExpr x) {
        return new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT);
    }

}
