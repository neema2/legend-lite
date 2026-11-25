// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.result.CellText;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * THE PRESENTER: LEGEND-ENGINE'S AND LEGEND-PURE'S TEXT OF A TYPED VALUE (Phase 3 of
 * docs/records/total-wall-edge-program-2026-11-16.md, 2026-11-16; {@code EngineCells} until then, W8 of
 * docs/records/d-plan-2026-11-16.md): the one formatter family the compatibility edge spells a value with, in Java
 * over typed values, never the database's printed text -- legend-engine's JSON token of a cell ({@code pure/v1}
 * execute's, its service tests' and its TDS objects': {@link #engine}), a pivot column's presented name
 * ({@link #presentedName}), legend-pure's date text the {@code pure/v1} Arrow route hands its writer
 * ({@link #LEGEND_PURE}) and legend-engine's text of an empty TDS cell ({@link #TDS_NULL}); legend-pure's print of a
 * value, a second Float printer with no caller since the catalog's sentences (BE, 84a063611), was deleted by B2b's
 * design audit of 2026-11-25 (G1: a relation's cell is printed by {@link PureValueText}, legend-pure's own
 * DecimalFormat; a Float inside a list or a map under {@link #LEGEND_PURE} is still {@link CellText#element}'s
 * {@code carriedFloat}, which differs on values such as {@code 1e23}, recorded in B2b's record). The
 * product writes its own forms ({@link CellText}); the rules here are the same serialization rule, never a
 * computation: each chooses digits for a value it holds whole, converts no kind, shifts no zone and defaults no null.
 */
public final class Presenter {

    private Presenter() {
    }

    /** legend-engine's text of an empty TDS cell, which its {@code makeString} and a TDS cell's {@code toString} print
     *  for an empty element (the product's join skips one, its cell's text is empty: register S313, S316; phase D's
     *  D1.3). */
    public static final String TDS_NULL = "TDSNull";

    /** legend-engine's JSON token of a cell of column {@code c} ({@code pure/v1} execute, measured 4.145.0): null as
     *  {@code null}; an Integer's digits; a Float as Java's {@code Double.toString} writes the double, and a NaN or an
     *  infinite one {@code null} ({@link #engineCell}); a Decimal at its own scale (register S104); a boolean; a text
     *  as a string; a StrictDate's day, and a DateTime's nine fraction digits with {@code +0000}, as strings; a Variant
     *  as a string holding its JSON (measured, 4.145.0, 2026-10-28: PARK-34's probe; the engine re-spells the JSON's
     *  numbers through its JSON library, lite keeps the database's digits: register S115); a structure as its own
     *  JSON. */
    public static Json.Node engine(@com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
        if (cell == null) {
            return Json.nil();
        }
        return switch (cell) {
            case Long l -> Json.num(l);
            case BigInteger b -> Json.Num.ofToken(b.toString(), new BigDecimal(b));
            case Double d -> engineCell(d);
            case BigDecimal d -> isFloat(c) ? engineCell(d.doubleValue()) : Json.Num.ofToken(d.toPlainString(), d);
            case Boolean b -> Json.bool(b);
            case String s -> Json.str(s);
            case PureDateLiteral d -> Json.str(engineDay(d, c) ? CellText.dayText(d) : engineJson(d));
            case PureTimeLiteral t -> Json.str(t.canonicalText());
            case Map<?, ?> m -> structure(CellText.declared(m), c);
            case List<?> l -> structure(CellText.declared(l), c);
            default -> throw refused(cell, c);
        };
    }

    /** {@link #engine}'s token of a cell written straight into {@code w}: the same choices and text as the node
     *  ({@code CellTextDirectTest} holds the two forms equal). */
    public static void writeEngine(Json.Writer w, @com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
        switch (cell) {
            case null -> w.writeNull();
            case Long l -> w.writeLong(l);
            case BigInteger b -> w.writeToken(b.toString());
            // a Float's spelling decided in one place, its node's ({@link #engine}): a double, and a decimal the
            // database holds under a Float column
            case Double d -> Json.write(w, engine(d, c));
            case BigDecimal d when isFloat(c) -> Json.write(w, engine(d, c));
            case BigDecimal d -> w.writeToken(d.toPlainString());
            case Boolean b -> w.writeBool(b);
            case String s -> w.writeString(s);
            case PureDateLiteral d -> w.writeString(engineDay(d, c) ? CellText.dayText(d) : engineJson(d));
            case PureTimeLiteral t -> w.writeString(t.canonicalText());
            case Map<?, ?> m -> Json.write(w, structure(CellText.declared(m), c));
            case List<?> l -> Json.write(w, structure(CellText.declared(l), c));
            default -> throw refused(cell, c);
        }
    }

    /** A structure's JSON in legend-engine's tokens (an object's declared properties, a List's elements, a Pair's, a
     *  Map's entries), each value by the rules of a cell of its own kind. A nested value has no column of its own, so
     *  a date is its own text where it has no time of day, else a timestamp's. */
    private static Json.Node structure(@com.legend.base.Nullable Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case null -> Json.nil();
            case Map<?, ?> m -> {
                java.util.LinkedHashMap<String, Json.Node> fields = new java.util.LinkedHashMap<>();
                m.forEach((k, x) -> fields.put(String.valueOf(k), structure(x, c)));
                yield new Json.Obj(fields);
            }
            case List<?> l -> new Json.Arr(l.stream().map(x -> structure(x, c)).toList());
            case Long n -> Json.num(n);
            case Integer n -> Json.num(n);
            case BigInteger b -> Json.Num.ofToken(b.toString(), new BigDecimal(b));
            // a NaN or an infinite Float as its name, a string (register S119, pending the user's ruling)
            case Double d -> engineFloat(d);
            case BigDecimal d -> Json.Num.ofToken(d.toPlainString(), d);
            case Boolean b -> Json.bool(b);
            case String s -> Json.str(s);
            case PureDateLiteral d -> Json.str(!d.precision().atLeast(PureDateLiteral.Precision.HOUR)
                    ? d.canonicalText() : engineJson(d));
            case PureTimeLiteral t -> Json.str(t.canonicalText());
            default -> throw refused(v, c);
        };
    }

    /** {@code name} as legend-engine labels a column (its {@code mayQuotePivotColNames}, pureToSQLQuery.pure:2985;
     *  the compiler's {@code Type.RelationType.presentPivotName}): a name holding the pivot separator {@code __|__}
     *  that is not already quote-wrapped carries literal single quotes as part of the NAME. */
    public static String presentedName(String name) {
        return name.contains("__|__") && !(name.startsWith("'") && name.endsWith("'")) ? "'" + name + "'" : name;
    }

    /** legend-engine's token of a double: Java's {@code Double.toString}; a non-finite double, which JSON has no token
     *  for, as its name in a string -- the form its JSON library gives a NESTED value (Jackson's
     *  {@code QUOTE_NON_NUMERIC_NUMBERS}; not measured). A relation's cell is {@link #engineCell}'s. */
    private static Json.Node engineFloat(double d) {
        return Double.isFinite(d) ? Json.Num.ofToken(Double.toString(d), BigDecimal.valueOf(d))
                : Json.str(Double.toString(d));
    }

    /** legend-engine's JSON of a relation's Float CELL: the double's text ({@link #engineFloat}), and a NaN or an
     *  infinity {@code null}. Read from its source 2026-10-29 (S1.5, the upstream-fidelity tenet): its relational
     *  serializer ({@code RelationalResultToJsonDefaultSerializer.processRow}) runs a Float column's transformer, the
     *  identity ({@code SetImplTransformers.buildTransformer}), then
     *  {@code ExecutionResultObjectMapperFactory.convertPurePrimitiveToJson}, which writes {@code null} for
     *  {@code isNaN} or {@code isInfinite}. */
    private static Json.Node engineCell(double d) {
        return Double.isFinite(d) ? engineFloat(d) : Json.nil();
    }

    private static boolean isFloat(ExecutionPlan.Column c) {
        return c.type().equals("Float");
    }

    /** Whether legend-engine writes a date as its day: by what the database answered, never by the time of day --
     *  its serializer writes a {@code java.sql.Timestamp} in full and only a {@code java.sql.Date} as its day
     *  ({@code ValueTransformer}, legend-engine's relational execution), so a day without a time, or a column that
     *  holds days ({@link ExecutionPlan.Precision#DAY}: a StrictDate's, a DATE slot's). A Date at midnight in a
     *  TIMESTAMP column is a date-time like the column's others (2026-10-27, the result boundary program). */
    private static boolean engineDay(PureDateLiteral d, ExecutionPlan.Column c) {
        return !d.precision().atLeast(PureDateLiteral.Precision.HOUR) || c.precision() == ExecutionPlan.Precision.DAY;
    }

    /** legend-engine's JSON of a date (its ServiceTestRunner / PURE_TDSOBJECT value transformer, witnessed 2026-09-16
     *  on the stress corpus): a date-only value its literal body; a time-bearing value its seconds, NINE fraction
     *  digits and the GMT {@code +0000} suffix whatever precision was written --
     *  {@code 2024-06-03T09:07:00.000000000+0000}. Moved from {@code PureDateLiteral.toEngineJson} (W8, 2026-11-16). */
    static String engineJson(PureDateLiteral d) {
        return d.precision().atLeast(PureDateLiteral.Precision.HOUR) ? d.toNineDigits() + "+0000" : d.toNineDigits();
    }

    // ---- legend-pure's date text ----------------------------------------------------------------------------------

    /** legend-pure's text of a date in a cell, which the {@code pure/v1} Arrow route hands its writer (W4's audit,
     *  2026-11-16, DoD-2: an edge answer does not move with the product's S312): a value with no time of day, or a
     *  column that holds days, as its day; a value carried at the precision it was written, a nested value and a mixed
     *  column's winner at the precision its digits show, with {@code +0000} from a minute on; a relation's timestamp
     *  cell at nine fraction digits and {@code +0000}. Handed to a writer the edge's receiver makes (W8: a run is
     *  handed the receiver, never this text). */
    public static final CellText.Forms LEGEND_PURE = new CellText.Forms() {
        @Override
        public String date(PureDateLiteral d, String type, ExecutionPlan.Column c, boolean nested) {
            return legendPure(d, type, c, nested);
        }

        /** legend-pure's text of a List (the prelude's List.toString(): its values' texts joined, joinStrings('[',
         *  ', ', ']')) and of a Map (its entries as 'key=value' in braces, comma-joined: its Java map's text), moved here
         *  from result.CellText (the audit's K13, 2026-11-17: the product writes each as its JSON). */
        @Override
        public String structure(Object value, String type, ExecutionPlan.Column c) {
            java.util.List<String> args = CellText.typeArguments(type);
            StringBuilder out = new StringBuilder();
            if (value instanceof java.util.List<?> l) {
                out.append('[');
                for (int i = 0; i < l.size(); i++) {
                    out.append(i == 0 ? "" : ", ").append(CellText.element(l.get(i), args.get(0), c, this));
                }
                return out.append(']').toString();
            }
            java.util.Map<?, ?> m = (java.util.Map<?, ?>) value;
            out.append('{');
            for (java.util.Map.Entry<?, ?> e : m.entrySet()) {
                out.append(out.length() > 1 ? ", " : "").append(CellText.element(e.getKey(), args.get(0), c, this))
                        .append('=').append(CellText.element(e.getValue(), args.get(1), c, this));
            }
            return out.append('}').toString();
        }
    };

    private static String legendPure(PureDateLiteral d, String type, ExecutionPlan.Column c, boolean nested) {
        if (nested) {
            return pureZoned(d.trimmed());
        }
        boolean own = type.equals(c.type());
        ExecutionPlan.Precision p = c.precision();
        if (p == ExecutionPlan.Precision.WRITTEN) {
            return pureZoned(d);
        }
        if (!d.precision().atLeast(PureDateLiteral.Precision.HOUR) || type.equals("StrictDate")
                || (own && p == ExecutionPlan.Precision.DAY)) {
            return CellText.dayText(d);
        }
        if (own && p == ExecutionPlan.Precision.INSTANT) {
            return engineJson(d);
        }
        return pureZoned(d.trimmed());
    }

    /** A date at its own precision, legend-pure's {@code +0000} from a minute on (an hour-only date has none). */
    private static String pureZoned(PureDateLiteral d) {
        return d.precision().atLeast(PureDateLiteral.Precision.MINUTE) ? d.canonicalText() + "+0000"
                : d.canonicalText();
    }

    private static IllegalArgumentException refused(Object cell, ExecutionPlan.Column c) {
        return new IllegalArgumentException("column '" + c.name() + "' (" + c.type() + ", slot " + c.slot() + "): "
                + "legend-engine's JSON token of a " + cell.getClass().getSimpleName() + " cell is not defined ("
                + cell + ")");
    }
}
