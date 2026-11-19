// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * A typed cell's TEXT at the edge (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R3, R5; 2026-10-27): the spellings the
 * writers use, each a function of the session's carrier and the plan's column facts alone. A spelling is
 * serialization, never computation: it chooses digits for a value it holds whole (a Float's shortest digits, a
 * Decimal's own scale, a DateTime's fraction), converts no kind, shifts no zone and defaults no null. The product's
 * vocabulary: Pure's print forms ({@link #pure}) and lite's JSON tokens ({@link #pureJson}). legend-engine's JSON
 * tokens and legend-pure's date text are the compatibility edge's ({@code edge.Presenter}, W8 of
 * docs/records/d-plan-2026-11-16.md, 2026-11-16), which composes {@link #declared}, {@link #number} and
 * {@link #dayText}. What the database spelled for these until the result boundary program's phase 3 (the lowering's
 * CSV and JSON wires, legend-engine's Float and DateTime text in SQL) is the same rule, written where it belongs;
 * those spellings are deleted.
 */
public final class CellText {

    private CellText() {
    }

    /** Pure's text of a non-null cell of column {@code c}: an Integer's digits; a Float's shortest digits laid out as
     *  Pure prints them (a point always, never an exponent); a Decimal at its own scale; a boolean; a text as it is;
     *  a StrictDate's day; a DateTime as the product writes one ({@link #productTime}); a structure or a text-carried value as its
     *  own text. */
    public static String pure(Object cell, ExecutionPlan.Column c) {
        return pure(cell, c.type(), c, PRODUCT);
    }

    /** {@link #pure} with a date's text written by {@code dates}: the product's ({@link #PRODUCT}), or the one a
     *  compatibility route hands its writer (legend-pure's, the edge's; W4's audit, 2026-11-16). */
    public static String pure(Object cell, ExecutionPlan.Column c, Forms dates) {
        return pure(cell, c.type(), c, dates);
    }

    /** A CELL'S FORMS (the audit's K13, 2026-11-17; a date's text alone until then, {@code DateText}, W4's audit,
     *  2026-11-16): the product's ({@link #PRODUCT}: register S312's dates, a List's and a Map's text their JSON, the
     *  one text the JSON output writes, the user's ruling of 2026-11-10), or legend-pure's ({@code
     *  edge.Presenter.LEGEND_PURE}), which the edge's receivers hand the writer they make (DoD-2: an edge answer does
     *  not move with the product's). No default: each form answers both. */
    public interface Forms {
        /** {@code d}'s text: {@code type} the Pure type it is read under, {@code c} its column, {@code nested} whether
         *  it is a structure's element (which has no column of its own). */
        String date(PureDateLiteral d, String type, ExecutionPlan.Column c, boolean nested);

        /** A List's or a Map's text, {@code value} of Pure type {@code type} in column {@code c}. */
        String structure(Object value, String type, ExecutionPlan.Column c);
    }

    /** The product's forms: S312's dates; a List's and a Map's text the JSON its JSON output writes. */
    public static final Forms PRODUCT = new Forms() {
        @Override
        public String date(PureDateLiteral d, String type, ExecutionPlan.Column c, boolean nested) {
            return nested ? productNested(d) : productDate(d, type, c);
        }

        @Override
        public String structure(Object value, String type, ExecutionPlan.Column c) {
            return Json.toCompact(CellText.structure(declared(value), c, this));
        }
    };

    /** {@link #pure} of a cell, or a structure's element, of Pure type {@code type} ({@code c} the column it is in). */
    private static String pure(Object cell, String type, ExecutionPlan.Column c, Forms dates) {
        String collection = collectionClass(type);
        return switch (cell) {
            case Long l -> Long.toString(l);
            case BigInteger b -> b.toString();
            case Double d -> carriedFloat(d);
            case BigDecimal d -> type.equals("Float") ? carriedFloat(d) : d.toPlainString();
            case Boolean b -> b.toString();
            case String s -> s;
            case PureDateLiteral d -> dates.date(d, type, c, false);
            case PureTimeLiteral t -> t.canonicalText();
            // a List's text, the forms' (the audit's K13, 2026-11-17): the product's its JSON array, legend-pure's the
            // prelude's List.toString() (joinStrings('[', ', ', ']'), the edge's)
            case List<?> l when collection.equals("List") -> dates.structure(l, type, c);
            // a Pair prints '<first, second>' (the prelude's Pair.toString())
            case Map<?, ?> m when collection.equals("Pair") -> {
                List<String> args = typeArguments(type);
                yield "<" + element(m.get("first"), args.get(0), c, dates) + ", "
                        + element(m.get("second"), args.get(1), c, dates) + ">";
            }
            // a Map's text, the forms' (the audit's K13, 2026-11-17): the product's its JSON object, legend-pure's its
            // entries as 'key=value' in braces (the edge's)
            case Map<?, ?> m when collection.equals("Map") -> dates.structure(m, type, c);
            // a user class's instance prints its repository id, interpreted Pure's anonymous instance name (the
            // lowering's own rule, Scalars.pureToString: "@_" and the instance's identity)
            // an instance's text is refused in the product (register S314: objects are refused, pointing to graph
            // fetch; W4 of docs/records/d-plan-2026-11-16.md): legend-pure's anonymous instance name is the edge's
            case Map<?, ?> m when m.containsKey(SYNTHETIC_ID) -> throw refused(cell, c,
                    "lite's text (an instance's text is refused: fetch it with graphFetch)");
            default -> throw refused(cell, c, "Pure's text");
        };
    }

    /** A structure's element as Pure prints it inside its parent's text. A Pure collection holds no null, so a null
     *  element is a decode fault, refused by name rather than printed as some text. */
    /** An element's text in a structure of column {@code c} (a null has none), under {@code dates}: the edge's
     *  legend-pure form reads it. */
    public static String element(@com.legend.base.Nullable Object v, String type, ExecutionPlan.Column c,
            Forms dates) {
        if (v == null) {
            throw new IllegalArgumentException("column '" + c.name() + "' (" + c.type() + "): a null " + type
                    + " inside a structure has no Pure text");
        }
        return pure(v, type, c, dates);
    }

    /** The platform collection class a Pure type names ({@code List}, {@code Pair}, {@code Map}), else the empty
     *  text: the class's name with or without its package, before its type arguments. */
    private static String collectionClass(String type) {
        int lt = type.indexOf('<');
        if (lt < 0) {
            return "";
        }
        String name = type.substring(0, lt).trim();
        if (name.startsWith(COLLECTION_PACKAGE)) {
            name = name.substring(COLLECTION_PACKAGE.length());
        }
        return name.equals("List") || name.equals("Pair") || name.equals("Map") ? name : "";
    }

    private static final String COLLECTION_PACKAGE = "meta::pure::functions::collection::";

    /** The type arguments of a generic type's text, split at its top-level commas ({@code Pair<Pair<Integer, String>,
     *  Integer>} is {@code Pair<Integer, String>} and {@code Integer}). */
    public static List<String> typeArguments(String type) {
        int lt = type.indexOf('<');
        if (lt < 0 || !type.endsWith(">")) {
            throw new IllegalArgumentException("'" + type + "' has no type arguments");
        }
        List<String> out = new java.util.ArrayList<>();
        int depth = 0;
        int start = lt + 1;
        for (int i = start; i < type.length() - 1; i++) {
            char ch = type.charAt(i);
            if (ch == '<') {
                depth++;
            } else if (ch == '>') {
                depth--;
            } else if (ch == ',' && depth == 0) {
                out.add(type.substring(start, i).trim());
                start = i + 1;
            }
        }
        out.add(type.substring(start, type.length() - 1).trim());
        return out;
    }

    /** A structure's JSON (an object's declared properties, a List's elements, a Pair's, a Map's entries), each value
     *  by the rules of a cell of its own kind -- a float, a date, a time spelled as one is, never as the JSON library
     *  would spell a Java value: Pure's tokens (legend-engine's are the edge's, {@code edge.Presenter}). A nested
     *  value has no column of its own, so a date is its day where it has no time of day, else a timestamp's text. */
    private static Json.Node structure(@com.legend.base.Nullable Object v, ExecutionPlan.Column c, Forms dates) {
        return switch (v) {
            case null -> Json.nil();
            case Map<?, ?> m -> {
                java.util.LinkedHashMap<String, Json.Node> fields = new java.util.LinkedHashMap<>();
                m.forEach((k, x) -> fields.put(String.valueOf(k), structure(x, c, dates)));
                yield new Json.Obj(fields);
            }
            case List<?> l -> new Json.Arr(l.stream().map(x -> structure(x, c, dates)).toList());
            case Long n -> Json.num(n);
            case Integer n -> Json.num(n);
            case BigInteger b -> token(b.toString(), new BigDecimal(b));
            // a NaN or an infinite Float as its name, a string (register S119, pending the user's ruling)
            case Double d -> Double.isFinite(d) ? token(carriedFloat(d), BigDecimal.valueOf(d)) : Json.str(carriedFloat(d));
            case BigDecimal d -> token(d.toPlainString(), d);
            case Boolean b -> Json.bool(b);
            case String s -> Json.str(s);
            case PureDateLiteral d -> Json.str(dates.date(d, "Date", c, true));
            case PureTimeLiteral t -> Json.str(t.canonicalText());
            default -> throw refused(v, c, "Pure's JSON token");
        };
    }

    /** Lite's own JSON token of a cell (its CSV's and JSON's values are Pure's): null as {@code null}; an Integer's
     *  digits; a Float as Pure's text; a Decimal at its scale; a boolean; a text as a string; a date kind as a string
     *  of the product's text; a structure as its own JSON. */
    public static Json.Node pureJson(@com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
        return pureJson(cell, c, PRODUCT);
    }

    /** {@link #pureJson} with a date's text written by {@code dates} ({@link #pure}'s note). */
    public static Json.Node pureJson(@com.legend.base.Nullable Object cell, ExecutionPlan.Column c, Forms dates) {
        if (cell == null) {
            return Json.nil();
        }
        return switch (cell) {
            // a NaN or an infinite Float as its name, a string (register S119, pending the user's ruling)
            case Double d -> Double.isFinite(d) ? token(carriedFloat(d), BigDecimal.valueOf(d)) : Json.str(carriedFloat(d));
            case BigDecimal d -> token(isFloat(c) ? carriedFloat(d) : d.toPlainString(), d);
            case Long l -> Json.num(l);
            case BigInteger b -> token(b.toString(), new BigDecimal(b));
            case Boolean b -> Json.bool(b);
            case String s -> Json.str(s);
            case PureDateLiteral d -> Json.str(dates.date(d, c.type(), c, false));
            case PureTimeLiteral t -> Json.str(t.canonicalText());
            case Map<?, ?> m -> structure(declared(m), c, dates);
            case List<?> l -> structure(declared(l), c, dates);
            default -> throw refused(cell, c, "Pure's JSON token");
        };
    }

    /**
     * {@link #pureJson}'s token of a cell written straight into {@code w} (the tier 1 of the product path's payoff,
     * 2026-11-01: a node made per cell and copied was a third of the JSON rows writer's time): the same choices, the
     * same text -- a number's token where the node would carry one ({@link #token}'s rule writes a whole number's
     * {@code long}, whose digits are the token's own), a structure through its node as before.
     * {@code CellTextDirectTest} holds the two forms equal.
     */
    public static void writePureJson(Json.Writer w, @com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
        switch (cell) {
            case null -> w.writeNull();
            // a Float's spelling decided in one place: its node's ({@link #pureJson})
            case Double d -> Json.write(w, pureJson(d, c));
            case BigDecimal d -> w.writeToken(isFloat(c) ? carriedFloat(d) : d.toPlainString());
            case Long l -> w.writeLong(l);
            case BigInteger b -> w.writeToken(b.toString());
            case Boolean b -> w.writeBool(b);
            case String s -> w.writeString(s);
            case PureDateLiteral d -> w.writeString(productDate(d, c.type(), c));
            case PureTimeLiteral t -> w.writeString(t.canonicalText());
            case Map<?, ?> m -> Json.write(w, structure(declared(m), c, PRODUCT));
            case List<?> l -> Json.write(w, structure(declared(l), c, PRODUCT));
            default -> throw refused(cell, c, "Pure's JSON token");
        }
    }

    // ---- Float ----------------------------------------------------------------------------------------------------

    /** A CARRIED Float's text (named for carriage, Phase 3 of docs/records/total-wall-edge-program-2026-11-16.md,
     *  2026-11-16: the value a CSV or JSON cell carries, read back as the same double -- §10 item 3 -- and the one rule
     *  the compatibility edge's presenter prints a Float by; the product's {@code toString} of a Float is S311's six
     *  decimals, in the query). Pure's layout of a double: its shortest digits ({@code Double.toString}'s, which read back as the same double),
     *  an exponent laid out positionally with a point always ({@code 1500000000000000.0}, {@code 0.00000025}); NaN and
     *  the infinities as Java names them. A negative zero keeps its sign, {@code -0.0}: it is another double (the
     *  user, 2026-10-28; Pure's own {@code toString} writes {@code 0.0} for either, as lite's SQL does: register
     *  S117). The exponent form's mantissa carries one trailing zero where its digits are one ({@code 1.0E-4}), which
     *  is no digit of the value: stripped before the layout, so {@code 1.0E-4} is {@code 0.0001}, legend-pure's own
     *  text, not {@code 0.00010} (the user, 2026-10-28: a defect, fixed; the start page's S1.4 item 2). The strip is a
     *  representation operation: no rounding, the value whole. */
    public static String carriedFloat(double d) {
        String shortest = Double.toString(d);
        if (shortest.indexOf('E') < 0) {
            return shortest;
        }
        String plain = new BigDecimal(shortest).stripTrailingZeros().toPlainString();
        return plain.contains(".") ? plain : plain + ".0";
    }

    /** A carried Float's text ({@link #carriedFloat(double)}) where the database holds it exactly (a decimal slot under a Float column): its digits without
     *  trailing zeros, a point always. */
    public static String carriedFloat(BigDecimal d) {
        String plain = d.stripTrailingZeros().toPlainString();
        return plain.contains(".") ? plain : plain + ".0";
    }

    private static boolean isFloat(ExecutionPlan.Column c) {
        return c.type().equals("Float");
    }

    // ---- dates ----------------------------------------------------------------------------------------------------

    /** Lite's text of a date (W4 of docs/records/d-plan-2026-11-16.md, 2026-11-16; the product's DateTime text,
     *  register S312: UTC with {@code Z}, never legend-pure's {@code +0000}, which is the edge's and the judges'): a
     *  value with no time of day (a year, a month, a day, a StrictDate) or a column that holds days
     *  ({@link ExecutionPlan.Precision#DAY}) as its day; every time-bearing value -- a relation's timestamp cell, a value
     *  carried at the precision it was written, a value root's instant, a mixed column's winner, a nested value -- as
     *  S312's six fraction digits and Z ({@link #productTime}), the one form the SQL's {@code DATETIME_TO_TEXT} writes
     *  too (the orchestrator's W4 audit, 2026-11-16: "six digits and Z for every time-bearing product cell"; a value's
     *  written precision is the edge's, legend-pure's print form, the plan's decision log). */
    private static String productDate(PureDateLiteral d, String type, ExecutionPlan.Column c) {
        boolean own = type.equals(c.type());
        if (!d.precision().atLeast(PureDateLiteral.Precision.HOUR) || type.equals("StrictDate")
                || (own && c.precision() == ExecutionPlan.Precision.DAY)) {
            return dayText(d);
        }
        return productTime(d);
    }

    /** A structure's date as the product writes it: its day where it has no time of day, else S312's text. */
    private static String productNested(PureDateLiteral d) {
        return d.precision().atLeast(PureDateLiteral.Precision.HOUR) ? productTime(d) : dayText(d);
    }

    /** The product's text of a relation's timestamp cell (register S312, the user's ruling of 2026-11-10: "ISO 8601
     *  in UTC, six fraction digits and Z"; W4, 2026-11-16): what the SQL's {@code DATETIME_TO_TEXT} writes, so a cell
     *  and a computed text agree. */
    static String productTime(PureDateLiteral d) {
        // the value's own spelling at nine fraction digits: six and Z (S312), or all nine where the value is finer than
        // a microsecond -- a cell CARRIES its value, read back exactly (the orchestrator's MUST of 2026-11-16, the first
        // full CI's Cypher parity: S312 cut every cell to six digits, and a Neo4j value's nanoseconds were lost)
        return d.toCarriedDigits() + "Z";
    }

    /** A date's day ({@code yyyy-MM-dd}), or its own text where it is coarser than a day. */
    public static String dayText(PureDateLiteral d) {
        java.time.LocalDateTime t = d.toInstantFloor();
        return d.precision().atLeast(PureDateLiteral.Precision.DAY)
                ? String.format(java.util.Locale.ROOT, "%d-%02d-%02d", t.getYear(), t.getMonthValue(),
                        t.getDayOfMonth())
                : d.canonicalText();
    }

    // ---- structures -----------------------------------------------------------------------------------------------

    /** The identity fields an object carries through the database (docs/OBJECT_IDENTITY_2026_10_20.md §3.4; the
     *  compiler's {@code ClassLayouts.SYNTHETIC_ID}, {@code SYNTHETIC_TYPE}, {@code SYNTHETIC_CANON}). */
    static final String SYNTHETIC_ID = "__id";
    private static final String SYNTHETIC_TYPE = "__type";
    private static final String SYNTHETIC_CANON = "__canon";

    /** Whether {@code field} is one of the identity fields an object carries through the database. */
    public static boolean synthetic(String field) {
        return field.equals(SYNTHETIC_ID) || field.equals(SYNTHETIC_TYPE) || field.equals(SYNTHETIC_CANON);
    }

    /** A structure as a reader sees it (the lowering's {@code MixedEncoding.declaredOnly}, the same rule): an object
     *  its declared properties, its identity fields dropped -- its class alone where it declares none -- and every
     *  nested value the same. */
    public static Object declared(Object v) {
        if (v instanceof Map<?, ?> m) {
            Map<String, Object> out = new java.util.LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                String k = String.valueOf(e.getKey());
                if (!k.equals(SYNTHETIC_ID) && !k.equals(SYNTHETIC_TYPE) && !k.equals(SYNTHETIC_CANON)) {
                    out.put(k, e.getValue() == null ? null : declared(e.getValue()));
                }
            }
            if (out.isEmpty() && m.containsKey(SYNTHETIC_TYPE)) {
                out.put(SYNTHETIC_TYPE, m.get(SYNTHETIC_TYPE));
            }
            return out;
        }
        if (v instanceof List<?> l) {
            List<Object> out = new java.util.ArrayList<>(l.size());
            for (Object x : l) {
                out.add(x == null ? null : declared(x));
            }
            return out;
        }
        return v;
    }

    // ---- the rest -------------------------------------------------------------------------------------------------

    /** A number written as {@code token}, its exact value beside it ({@link Json.Num#ofToken}: the JSON library's since
     *  J36, 2026-11-18, where {@code number} stood public for the edge's Presenter alone). */
    private static Json.Num token(String token, BigDecimal value) {
        return Json.Num.ofToken(token, value);
    }

    private static IllegalArgumentException refused(Object cell, ExecutionPlan.Column c, String what) {
        return new IllegalArgumentException("column '" + c.name() + "' (" + c.type() + ", slot " + c.slot() + "): "
                + what + " of a " + cell.getClass().getSimpleName() + " cell is not defined (" + cell + ")");
    }
}
