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
 * Decimal's own scale, a DateTime's fraction), converts no kind, shifts no zone and defaults no null. Two vocabularies:
 * Pure's print forms ({@link #pure}), lite's own outputs' and interpreted Pure's; and legend-engine's JSON tokens
 * ({@link #engine}), its {@code pure/v1} execute's. What the database spelled for these until the result boundary
 * program's phase 3 (the lowering's CSV and JSON wires, legend-engine's Float and DateTime text in SQL) is the same
 * rule, written where it belongs; those spellings are deleted.
 */
public final class CellText {

    private CellText() {
    }

    /** Pure's text of a non-null cell of column {@code c}: an Integer's digits; a Float's shortest digits laid out as
     *  Pure prints them (a point always, never an exponent); a Decimal at its own scale; a boolean; a text as it is;
     *  a StrictDate's day; a DateTime at its precision with {@code +0000}; a structure or a text-carried value as its
     *  own text. */
    public static String pure(Object cell, ExecutionPlan.Column c) {
        return pure(cell, c.type(), c);
    }

    /** {@link #pure} of a cell, or a structure's element, of Pure type {@code type} ({@code c} the column it is in). */
    private static String pure(Object cell, String type, ExecutionPlan.Column c) {
        String collection = collectionClass(type);
        return switch (cell) {
            case Long l -> Long.toString(l);
            case BigInteger b -> b.toString();
            case Double d -> pureFloat(d);
            case BigDecimal d -> type.equals("Float") ? pureFloat(d) : d.toPlainString();
            case Boolean b -> b.toString();
            case String s -> s;
            case PureDateLiteral d -> pureDate(d, type, c);
            case PureTimeLiteral t -> t.toEngineString();
            // a List prints its values' texts joined (the prelude's List.toString(): joinStrings('[', ', ', ']'))
            case List<?> l when collection.equals("List") -> joined(l, typeArguments(type).get(0), c, "[", "]");
            // a Pair prints '<first, second>' (the prelude's Pair.toString())
            case Map<?, ?> m when collection.equals("Pair") -> {
                List<String> args = typeArguments(type);
                yield "<" + element(m.get("first"), args.get(0), c) + ", " + element(m.get("second"), args.get(1), c)
                        + ">";
            }
            // a Map prints its entries as 'key=value' in braces, comma-joined: legend-pure's map's own text (its Java
            // map's), the form every output wrote it in until now
            case Map<?, ?> m when collection.equals("Map") -> {
                List<String> args = typeArguments(type);
                StringBuilder out = new StringBuilder("{");
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (out.length() > 1) {
                        out.append(", ");
                    }
                    out.append(element(e.getKey(), args.get(0), c)).append('=')
                            .append(element(e.getValue(), args.get(1), c));
                }
                yield out.append('}').toString();
            }
            // a user class's instance prints its repository id, interpreted Pure's anonymous instance name (the
            // lowering's own rule, Scalars.pureToString: "@_" and the instance's identity)
            case Map<?, ?> m when m.get(SYNTHETIC_ID) instanceof String id -> "@_" + id;
            default -> throw refused(cell, c, "Pure's text");
        };
    }

    /** A structure's element as Pure prints it inside its parent's text. A Pure collection holds no null, so a null
     *  element is a decode fault, refused by name rather than printed as some text. */
    private static String element(@com.legend.base.Nullable Object v, String type, ExecutionPlan.Column c) {
        if (v == null) {
            throw new IllegalArgumentException("column '" + c.name() + "' (" + c.type() + "): a null " + type
                    + " inside a structure has no Pure text");
        }
        return pure(v, type, c);
    }

    private static String joined(List<?> l, String elementType, ExecutionPlan.Column c, String open, String close) {
        StringBuilder out = new StringBuilder(open);
        for (int i = 0; i < l.size(); i++) {
            if (i > 0) {
                out.append(", ");
            }
            out.append(element(l.get(i), elementType, c));
        }
        return out.append(close).toString();
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
    static List<String> typeArguments(String type) {
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

    /** legend-engine's JSON token of a cell of column {@code c} ({@code pure/v1} execute, measured 4.145.0): null as
     *  {@code null}; an Integer's digits; a Float as Java's {@code Double.toString} writes the double, and a NaN or an
     *  infinite one {@code null} ({@link #engineCell}); a Decimal at its
     *  own scale (register S104); a boolean; a text as a string; a StrictDate's day, and a DateTime's nine fraction
     *  digits with {@code +0000}, as strings; a Variant as a string holding its JSON (measured, 4.145.0, 2026-10-28:
     *  PARK-34's probe; the engine re-spells the JSON's numbers through its JSON library, lite keeps the database's
     *  digits: register S115); a structure as its own JSON. */
    public static Json.Node engine(@com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
        if (cell == null) {
            return Json.nil();
        }
        return switch (cell) {
            case Long l -> Json.num(l);
            case BigInteger b -> token(b.toString(), new BigDecimal(b));
            case Double d -> engineCell(d);
            case BigDecimal d -> isFloat(c) ? engineCell(d.doubleValue()) : token(d.toPlainString(), d);
            case Boolean b -> Json.bool(b);
            case String s -> Json.str(s);
            case PureDateLiteral d -> Json.str(engineDay(d, c) ? day(d) : d.toEngineJson());
            case PureTimeLiteral t -> Json.str(t.toEngineString());
            case Map<?, ?> m -> structure(declared(m), false, c);
            case List<?> l -> structure(declared(l), false, c);
            default -> throw refused(cell, c, "legend-engine's JSON token");
        };
    }

    /** A structure's JSON (an object's declared properties, a List's elements, a Pair's, a Map's entries), each value
     *  by the rules of a cell of its own kind -- a float, a date, a time spelled as one is, never as the JSON library
     *  would spell a Java value: legend-engine's tokens, or Pure's where {@code pure}. A nested value has no column of
     *  its own, so a date is its day where it has no time of day, else a timestamp's text. */
    private static Json.Node structure(@com.legend.base.Nullable Object v, boolean pure, ExecutionPlan.Column c) {
        return switch (v) {
            case null -> Json.nil();
            case Map<?, ?> m -> {
                java.util.LinkedHashMap<String, Json.Node> fields = new java.util.LinkedHashMap<>();
                m.forEach((k, x) -> fields.put(String.valueOf(k), structure(x, pure, c)));
                yield new Json.Obj(fields);
            }
            case List<?> l -> new Json.Arr(l.stream().map(x -> structure(x, pure, c)).toList());
            case Long n -> Json.num(n);
            case Integer n -> Json.num(n);
            case BigInteger b -> token(b.toString(), new BigDecimal(b));
            // a NaN or an infinite Float as its name, a string (register S119, pending the user's ruling)
            case Double d -> !pure ? engineFloat(d)
                    : Double.isFinite(d) ? token(pureFloat(d), BigDecimal.valueOf(d)) : Json.str(pureFloat(d));
            case BigDecimal d -> token(d.toPlainString(), d);
            case Boolean b -> Json.bool(b);
            case String s -> Json.str(s);
            case PureDateLiteral d -> Json.str(pure ? zoned(d.trimmed())
                    : !d.precision().atLeast(PureDateLiteral.Precision.HOUR) ? d.toEngineString() : d.toEngineJson());
            case PureTimeLiteral t -> Json.str(t.toEngineString());
            default -> throw refused(v, c, pure ? "Pure's JSON token" : "legend-engine's JSON token");
        };
    }

    /** Lite's own JSON token of a cell (its CSV's and JSON's values are Pure's): a Float as Pure's text, a Decimal at
     *  its scale, a date kind as a string of Pure's text, the rest as {@link #engine} writes it. */
    public static Json.Node pureJson(@com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
        if (cell == null) {
            return Json.nil();
        }
        return switch (cell) {
            // a NaN or an infinite Float as its name, a string (register S119, pending the user's ruling)
            case Double d -> Double.isFinite(d) ? token(pureFloat(d), BigDecimal.valueOf(d)) : Json.str(pureFloat(d));
            case BigDecimal d -> token(isFloat(c) ? pureFloat(d) : d.toPlainString(), d);
            case PureDateLiteral d -> Json.str(pureDate(d, c.type(), c));
            case Map<?, ?> m -> structure(declared(m), true, c);
            case List<?> l -> structure(declared(l), true, c);
            default -> engine(cell, c);
        };
    }

    /** {@code name} as legend-engine labels a column (its {@code mayQuotePivotColNames}, pureToSQLQuery.pure:2985;
     *  the compiler's {@code Type.RelationType.presentPivotName}): a name holding the pivot separator {@code __|__}
     *  that is not already quote-wrapped carries literal single quotes as part of the NAME. */
    public static String presentedName(String name) {
        return name.contains("__|__") && !(name.startsWith("'") && name.endsWith("'")) ? "'" + name + "'" : name;
    }

    // ---- Float ----------------------------------------------------------------------------------------------------

    /** Pure's layout of a double: its shortest digits ({@code Double.toString}'s, which read back as the same double),
     *  an exponent laid out positionally with a point always ({@code 1500000000000000.0}, {@code 0.00000025}); NaN and
     *  the infinities as Java names them. A negative zero keeps its sign, {@code -0.0}: it is another double (the
     *  user, 2026-10-28; Pure's own {@code toString} writes {@code 0.0} for either, as lite's SQL does: register
     *  S117). The exponent form's mantissa carries one trailing zero where its digits are one ({@code 1.0E-4}), which
     *  is no digit of the value: stripped before the layout, so {@code 1.0E-4} is {@code 0.0001}, legend-pure's own
     *  text, not {@code 0.00010} (the user, 2026-10-28: a defect, fixed; the start page's S1.4 item 2). The strip is a
     *  representation operation: no rounding, the value whole. */
    static String pureFloat(double d) {
        String shortest = Double.toString(d);
        if (shortest.indexOf('E') < 0) {
            return shortest;
        }
        String plain = new BigDecimal(shortest).stripTrailingZeros().toPlainString();
        return plain.contains(".") ? plain : plain + ".0";
    }

    /** Pure's text of a Float the database holds exactly (a decimal slot under a Float column): its digits without
     *  trailing zeros, a point always. */
    static String pureFloat(BigDecimal d) {
        String plain = d.stripTrailingZeros().toPlainString();
        return plain.contains(".") ? plain : plain + ".0";
    }

    /** legend-engine's token of a double: Java's {@code Double.toString}; a non-finite double, which JSON has no token
     *  for, as its name in a string -- the form its JSON library gives a NESTED value (Jackson's
     *  {@code QUOTE_NON_NUMERIC_NUMBERS}; not measured). A relation's cell is {@link #engineCell}'s. */
    private static Json.Node engineFloat(double d) {
        return Double.isFinite(d) ? token(Double.toString(d), BigDecimal.valueOf(d)) : Json.str(Double.toString(d));
    }

    /** legend-engine's JSON of a relation's Float CELL: the double's text ({@link #engineFloat}), and a NaN or an
     *  infinity {@code null}. Read from its source 2026-10-29 (S1.5, the upstream-fidelity tenet): its relational
     *  serializer ({@code RelationalResultToJsonDefaultSerializer.processRow}) runs a Float column's transformer, the
     *  identity ({@code SetImplTransformers.buildTransformer}), then
     *  {@code ExecutionResultObjectMapperFactory.convertPurePrimitiveToJson}, which writes {@code null} for
     *  {@code isNaN} or {@code isInfinite}. Phase 2 wrote the name in a string here, naming the engine without reading
     *  it. */
    private static Json.Node engineCell(double d) {
        return Double.isFinite(d) ? engineFloat(d) : Json.nil();
    }

    private static boolean isFloat(ExecutionPlan.Column c) {
        return c.type().equals("Float");
    }

    // ---- dates ----------------------------------------------------------------------------------------------------

    /** Pure's text of a date, by the column's written-precision fact ({@link ExecutionPlan.Precision}, the plan's;
     *  until 2026-10-28 the writer matched the slot's spelling for it, the start page's S1.4 item 1; the spellings
     *  read from upstream 2026-10-29, the upstream-fidelity tenet): a value carried at the precision it was written
     *  (WRITTEN: a year, a month, a day, an hour ... a fraction) as it is, {@code +0000} from a minute on ({@link #zoned});
     *  a day (DAY,
     *  a StrictDate, a value with no time of day) as its day; a relation's cell (INSTANT) as legend-engine's TDS
     *  spells a time-bearing date, nine fraction digits whatever the carrier ({@link PureDateLiteral#toEngineJson}:
     *  its {@code ValueTransformer}; legend-pure's {@code fromSQLTimestamp} reads the same nine), never as a day (the
     *  midnight rule of 12b89c7d, a Date's midnight as its day, went 2026-10-29: no upstream prints a DateTime so);
     *  a value root's instant (MINIMAL), a nested value and a mixed column's winner at the precision its digits
     *  show, the fraction's trailing zeros dropped (register S118). Two choices the S1.4 audit asked to be written
     *  down (2026-10-29): in Pure's TEXT (the CSV path, {@link #element}) WRITTEN applies to a nested element too (a
     *  tagged slot's kind carries each element's written digits), where DAY and INSTANT are the
     *  column's own cell's facts ({@code own}) -- the JSON path's nested dates ({@link #structure}) take the digits
     *  shown whatever the column's fact, D9's open question (the S1.5 audit, finding 8); and MINIMAL and a null fact
     *  (a column with no temporal of its own: a mixed column's winner, a derived column) spell the same text, the
     *  digits shown -- S118 covers both, and a Date cell under a null fact is a value root's. */
    private static String pureDate(PureDateLiteral d, String type, ExecutionPlan.Column c) {
        boolean own = type.equals(c.type());
        ExecutionPlan.Precision p = c.precision();
        if (p == ExecutionPlan.Precision.WRITTEN) {
            return zoned(d);
        }
        if (!d.precision().atLeast(PureDateLiteral.Precision.HOUR) || type.equals("StrictDate")
                || (own && p == ExecutionPlan.Precision.DAY)) {
            return day(d);
        }
        if (own && p == ExecutionPlan.Precision.INSTANT) {
            return d.toEngineJson();
        }
        return zoned(d.trimmed());
    }

    /** A date at its own precision as legend-pure prints it: its text, and {@code +0000} from a MINUTE on --
     *  {@code DateFormat.append} (legend-pure m4) writes the zone inside its minute branch, so an hour-only date prints
     *  {@code 2014-03-10T16} (its {@code TestDateFormat}) and a minute {@code ...T16:12+0000}; legend-engine's
     *  {@code PureDateSerializer} writes the same text. Until 2026-10-29 the writer wrote the zone from an hour on
     *  (S1.5's classification, G5: a move to upstream's text). */
    private static String zoned(PureDateLiteral d) {
        return d.precision().atLeast(PureDateLiteral.Precision.MINUTE) ? d.toEngineString() + "+0000"
                : d.toEngineString();
    }

    /** Whether legend-engine writes a date as its day: by what the database answered, never by the time of day --
     *  its serializer writes a {@code java.sql.Timestamp} in full and only a {@code java.sql.Date} as its day
     *  ({@code ValueTransformer}, legend-engine's relational execution), so a day without a time, or a column that
     *  holds days ({@link ExecutionPlan.Precision#DAY}: a StrictDate's, a DATE slot's). A Date at midnight in a
     *  TIMESTAMP column is a date-time like the column's others (2026-10-27, the result boundary program: found by
     *  the PCT read as the engine's adapter reads it, which a column of both spellings failed). */
    private static boolean engineDay(PureDateLiteral d, ExecutionPlan.Column c) {
        return !d.precision().atLeast(PureDateLiteral.Precision.HOUR) || c.precision() == ExecutionPlan.Precision.DAY;
    }

    private static String day(PureDateLiteral d) {
        java.time.LocalDateTime t = d.toInstantFloor();
        return d.precision().atLeast(PureDateLiteral.Precision.DAY)
                ? String.format(java.util.Locale.ROOT, "%d-%02d-%02d", t.getYear(), t.getMonthValue(),
                        t.getDayOfMonth())
                : d.toEngineString();
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
    static Object declared(Object v) {
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

    /** A number written as {@code token}, its exact value {@code value} beside it. The JSON writer writes an integer
     *  number's {@code long} and any other number's token, so a whole number past a {@code long} (a HUGEINT's
     *  {@code 18446744073709551614}) is marked a non-integer and written by its token, never wrapped. */
    private static Json.Num token(String token, BigDecimal value) {
        boolean integral = token.chars().allMatch(ch -> ch == '-' || Character.isDigit(ch))
                && value.toBigInteger().bitLength() < 64;
        return new Json.Num(value.longValue(), value.doubleValue(), integral, value, token);
    }

    private static IllegalArgumentException refused(Object cell, ExecutionPlan.Column c, String what) {
        return new IllegalArgumentException("column '" + c.name() + "' (" + c.type() + ", slot " + c.slot() + "): "
                + what + " of a " + cell.getClass().getSimpleName() + " cell is not defined (" + cell + ")");
    }
}
