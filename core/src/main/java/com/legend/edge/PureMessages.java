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
            // lite's own refusals (S138, S181, the CSV width): no upstream text; the compatibility road writes the
            // product's former sentence, its parts the operands (a text written as it is, a value as its text)
            case VALUE_NOT_HELD, FRACTION_NOT_KEPT, CSV_TOO_WIDE -> parts(r);
            // [the source type's name, the target's]
            case CAST_IMPOSSIBLE -> new SqlExpr.StringLit("Cast exception: " + text(r, 0) + " cannot be cast to "
                    + text(r, 1));
            case RANGE_STEP_ZERO -> new SqlExpr.StringLit("range step must not be 0");
            // legend-pure's rows/_range assert, verbatim (#81's frame guard, merged 2026-11-14)
            case WINDOW_FRAME_INVERTED -> new SqlExpr.StringLit("Invalid window frame boundary - lower bound of window"
                    + " frame cannot be greater than the upper bound!");
            // lite's own sentence (S170: a bound's direction read from its shape), kept for the compatibility road
            case WINDOW_FRAME_OFFSET_INVALID -> new SqlExpr.StringLit("Invalid window frame boundary - a window frame"
                    + " bound written -(n) is n before the current row and any other n after it, so a computed n must"
                    + " be neither negative nor empty (docs/SEMANTICS_REGISTER.md S170)");
            // [the native's name]: lite's own sentence, kept for the compatibility road
            case LIST_LENGTHS_DIFFER -> new SqlExpr.StringLit(text(r, 0) + ": the two value lists differ in length");
            // [the constraint's id]: the engine's milestoning/constraint sentence, in a checked graph's defect row
            case CONSTRAINT_UNEVALUABLE -> new SqlExpr.StringLit("Unable to evaluate constraint [" + text(r, 0)
                    + "]: data not available - check your mappings");
            // DuckDB's own words, which the reference database's other dialects raise (the D0 inventory's K3)
            case MAP_KEYS_MISALIGNED -> new SqlExpr.StringLit("The map key list does not align with the map value list.");
            case MAP_KEY_NULL -> new SqlExpr.StringLit("Map keys can not be NULL.");
            case MAP_KEYS_NOT_UNIQUE -> new SqlExpr.StringLit("Map keys must be unique.");
            // [the text]: DuckDB's cast errors
            case TEXT_NOT_INTEGER -> SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                    new SqlExpr.StringLit("Could not convert string '"), r.operands().get(0),
                    new SqlExpr.StringLit("' to INT64"));
            case TEXT_NOT_DATE -> SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT, new SqlExpr.StringLit("Could not parse '"),
                    r.operands().get(0), new SqlExpr.StringLit("' as a date"));
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

    /** {@code r}'s operands as one text, in order: a text the planner knew as it is, a value as its text. */
    private static SqlExpr parts(SqlExpr.RaiseText r) {
        java.util.List<SqlExpr> o = r.operands();
        SqlExpr[] parts = new SqlExpr[o.size()];
        for (int i = 0; i < parts.length; i++) {
            parts[i] = o.get(i) instanceof SqlExpr.StringLit ? o.get(i) : str(o.get(i));
        }
        return parts.length == 1 ? parts[0] : cat(parts);
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
