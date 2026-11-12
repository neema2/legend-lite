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
            // engine's bitShiftLeft/Right.pure
            case SHIFT_OUT_OF_RANGE -> new SqlExpr.StringLit("Unsupported number of bits to shift - max bits allowed is 62");
            // [the part's label, the date]: legend-pure's 'Cannot get <part> for <date>'
            case DATE_PART_ABSENT -> cat(new SqlExpr.StringLit("Cannot get " + text(r, 0) + " for "),
                    str(r.operands().get(1)));
            // [the offset, the size]: legend-pure's at() out of bounds
            case INDEX_OUT_OF_BOUNDS -> cat(new SqlExpr.StringLit("The system is trying to get an element at offset "),
                    str(r.operands().get(0)), new SqlExpr.StringLit(" where the collection is of size "),
                    str(r.operands().get(1)));
            // lite's own sentence until D1.1 (no upstream text: the D0 inventory's 1.6), kept for the compatibility road
            case INSERT_OUT_OF_BOUNDS -> new SqlExpr.StringLit("add(set, index, value): index out of bounds");
            // [the value]: legend-pure's 'Unable to compute sqrt of <x>', <x> as Pure prints a Float (K2)
            case SQRT_OF_NEGATIVE -> cat(new SqlExpr.StringLit("Unable to compute sqrt of "),
                    SqlExpr.Call.of(com.legend.sql.SqlFn.FLOAT_PRINT, r.operands().get(0)));
            // [the bound] (a size known empty) or [the bound, the size]: 'Cannot cast a collection of size <n> to
            // multiplicity <bound>'
            case SIZE_NOT_MULTIPLICITY -> r.operands().size() == 1
                    ? new SqlExpr.StringLit("Cannot cast a collection of size 0 to multiplicity " + text(r, 0))
                    : new SqlExpr.Call(com.legend.sql.SqlFn.CONCAT, java.util.List.of(
                            new SqlExpr.StringLit("Cannot cast a collection of size "), str(r.operands().get(1)),
                            new SqlExpr.StringLit(" to multiplicity " + text(r, 0))));
            // [low, high]: legend-pure's slice
            case SLICE_BOUNDS_INVERTED -> SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                    SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                            SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT, new SqlExpr.StringLit("The low bound ("),
                                    str(r.operands().get(0))),
                            new SqlExpr.StringLit(") can't be higher than the high bound (")),
                    SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT, str(r.operands().get(1)),
                            new SqlExpr.StringLit(") in a slice operation")));
            case DATE_COMPONENT_INVALID -> dateComponent(r);
            // [the source type's name, the target's]
            case CAST_IMPOSSIBLE -> new SqlExpr.StringLit("Cast exception: " + text(r, 0) + " cannot be cast to "
                    + text(r, 1));
            case RANGE_STEP_ZERO -> new SqlExpr.StringLit("range step must not be 0");
            // [the native's name]: lite's own sentence, kept for the compatibility road
            case LIST_LENGTHS_DIFFER -> new SqlExpr.StringLit(text(r, 0) + ": the two value lists differ in length");
            case CONSTRAINT_UNEVALUABLE, MAP_KEYS_MISALIGNED, MAP_KEY_NULL,
                 MAP_KEYS_NOT_UNIQUE, TEXT_NOT_INTEGER, TEXT_NOT_DATE, VALUE_NOT_HELD, FRACTION_NOT_KEPT,
                 CSV_TOO_WIDE -> throw new UnsupportedOperationException("a raise of kind " + r.kind()
                    + " reached the edge: no site raises it yet (phase D's D1.1 moves its sites one family at a time)");
        };
    }

    /** date(...)'s component out of range: [the component, its literal] spelled at planning as legend-pure writes it
     *  ('Invalid month: 13', a fractional second's plain digits), [day, year, month, day] its attempted date
     *  ('Invalid day: 2016-12-32', the month unpadded), [the component, a value] the database's text of it. */
    private static SqlExpr dateComponent(SqlExpr.RaiseText r) {
        java.util.List<SqlExpr> o = r.operands();
        if (o.size() == 4) {
            return new SqlExpr.StringLit("Invalid day: " + ((SqlExpr.IntLit) o.get(1)).value() + "-"
                    + ((SqlExpr.IntLit) o.get(2)).value() + "-" + ((SqlExpr.IntLit) o.get(3)).value());
        }
        String head = "Invalid " + text(r, 0) + ": ";
        if (o.get(1) instanceof SqlExpr.IntLit lit) {
            return new SqlExpr.StringLit(head + lit.value());
        }
        if (o.get(1) instanceof SqlExpr.FloatLit fl) {
            return new SqlExpr.StringLit(head + java.math.BigDecimal.valueOf(fl.value()).toPlainString());
        }
        if (o.get(1) instanceof SqlExpr.DecimalLit dl) {
            return new SqlExpr.StringLit(head + dl.value().toPlainString());
        }
        // a value the database holds: its text
        return cat(new SqlExpr.StringLit(head), str(o.get(1)));
    }

    /** Operand {@code i}, a text the planner knew (a label, a type's name, a bound). */
    private static String text(SqlExpr.RaiseText r, int i) {
        return ((SqlExpr.StringLit) r.operands().get(i)).value();
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
        return new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR);
    }

}
