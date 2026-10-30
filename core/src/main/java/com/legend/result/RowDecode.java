// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE ROW DECODE (the plan path program's step 6, docs/PLAN_PATH_PROGRAM_2026_10_17.md §6, 2026-10-18): a tabular
 * result's row written as one JSON object -- a document's row (docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3: since
 * step 7 a JDBC session's cells come typed from the driver, {@link JdbcDecode}, the twin of this decode), a JSON cell
 * of such a row, or a list or a struct a database carries as JSON text -- read into cells by the plan's own columns -- each cell by its column's declared Pure type and, where the type alone does not say the
 * carrier (a {@code Number}, a literal-spelled or text-carried cell), by its slot. No model: the plan's three type
 * facts are the whole instruction. The cells are the carriers the host adjudication layer reads (the Z2 ruling's
 * carrier-decode bridge, docs/TENET_CHARTER.md Clause 2c): a date kind as {@link PureDateLiteral}, a time as
 * {@link PureTimeLiteral}, a Decimal as its exact {@link BigDecimal}, an Integer as a {@code long}, a Float as a
 * {@code double}, a class instance as the structure its JSON object spells. Nothing is computed; a token no arm reads
 * is refused by name, never guessed.
 */
public final class RowDecode {

    private RowDecode() {
    }

    /** The variant class's path (the plan's type vocabulary names a class by its path; no model here). */
    public static final String VARIANT = "meta::pure::metamodel::variant::Variant";

    /** One row's cells, in {@code columns}' order: each column read from the object's field of its name, or, when no
     *  field is named after any column and the counts agree, by position (a value root's one column, which the wire
     *  names after its output). */
    public static List<Object> cells(String rowJson, List<ExecutionPlan.Column> columns) {
        Json.Node parsed = Json.parse(rowJson);
        if (!(parsed instanceof Json.Obj row)) {
            throw new IllegalStateException("the statement wrote a row that is no JSON object: "
                    + excerpt(rowJson));
        }
        return cells(row, columns);
    }

    /** {@link #cells(String, List)} of a row already read as a JSON object. */
    public static List<Object> cells(Json.Obj row, List<ExecutionPlan.Column> columns) {
        boolean anyByName = columns.stream().anyMatch(c -> row.fields().containsKey(c.name()));
        boolean positional = !anyByName && row.fields().size() == columns.size();
        List<Json.Node> byPosition = positional ? new ArrayList<>(row.fields().values()) : List.of();
        List<Object> out = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            ExecutionPlan.Column c = columns.get(i);
            Json.Node node = positional ? byPosition.get(i) : row.fields().get(c.name());
            if (node == null) {
                throw new IllegalStateException("the row's object has no field for the plan's column '" + c.name()
                        + "' (it has " + row.fields().keySet() + "): the statement does not write the plan's columns");
            }
            if (slotWord(c.slot()).equals("TAGGED")) {
                // a tagged column (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a): its cell is the tag, the slot
                // index; the value stands in that slot's column beside it, read with the slot's own type
                Object tag = cell(node, tagOf(c));
                if (tag == null) {
                    out.add(null);
                    continue;
                }
                int index = ((Number) tag).intValue();
                ExecutionPlan.Column slot = taggedSlot(c, index);
                Json.Node held = row.fields().get(slot.name());
                if (held == null) {
                    throw new IllegalStateException("the row's object has no field for the tagged column '"
                            + c.name() + "'s slot " + slot.name() + " (it has " + row.fields().keySet() + ")");
                }
                Object value = cell(held, slot);
                if (value == null) {
                    throw new IllegalStateException("the row's tagged column '" + c.name() + "' names slot "
                            + slot.name() + ", and that slot's column is null");
                }
                // the tag rides into the batch beside the carrier (TaggedCell; track F, 2026-10-29)
                out.add(new TaggedCell(index, java.util.Objects.requireNonNull(presented(value, slot))));
                continue;
            }
            out.add(cell(node, c));
        }
        return out;
    }

    /** {@code node} as the cell of {@code c}: by the SLOT first -- its kind is the carrier's, as the legacy executor's
     *  label-driven unwrap read a driver's cell by the output's SQL type -- and by the declared Pure type where the
     *  slot is none, a structure's or JSON. A string token under a numeric slot is the TEXT the database holds unless
     *  the Pure type is itself numeric (the legacy executor kept a driver's String under an integral label: the
     *  engine's test store declares {@code interactionTable.ID INT} over a column its DDL creates VARCHAR, and the
     *  String-typed {@code Interaction.id} reads {@code '7'}; measured on the corpus, 2026-10-18); a numeric Pure type
     *  parses the token's digits, for a wire that quotes its numbers. */
    public static @com.legend.base.Nullable Object cell(Json.Node node, ExecutionPlan.Column c) {
        if (node instanceof Json.Null) {
            return null;
        }
        String type = c.type();
        if (type.equals(VARIANT)) {
            // a variant keeps its JSON text (the legacy executor's contract for a variant root)
            return Json.toCompact(node);
        }
        String slot = slotWord(c.slot());
        switch (slot) {
            // the slot labels that ARE the decode instruction: a text-carried temporal or decimal parses (the
            // LITERAL spelling carrier's arm left with track L's L2, 2026-10-29: a mixed value is a tagged value)
            case "TAGGED" -> throw new IllegalStateException("column '" + c.name() + "': a tagged cell is read with"
                    + " its row (its slot columns stand beside it), never alone");
            case "DECIMAL_TEXT" -> {
                return decimalText(text(node, c));
            }
            case "TEMPORAL_TEXT" -> {
                return temporalText(text(node, c), c);
            }
            case "DATE", "TIMESTAMP", "TIMESTAMPTZ" -> {
                return PureDateLiteral.parse(text(node, c).trim().replace(' ', 'T'));
            }
            case "TIME" -> {
                return PureTimeLiteral.parse(text(node, c).trim());
            }
            // the SQL kinds: the carrier is the slot's -- but a string token is the column's own text (above)
            case "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT" -> {
                return node instanceof Json.Str s && !numericPure(type) ? s.value() : integer(node, c);
            }
            case "DOUBLE", "FLOAT", "REAL" -> {
                return node instanceof Json.Str s && !numericPure(type) ? s.value() : floating(node, c);
            }
            case "DECIMAL", "NUMERIC" -> {
                return node instanceof Json.Str s && !numericPure(type) ? s.value() : decimal(node, c);
            }
            case "BOOLEAN" -> {
                if (node instanceof Json.Bool b) {
                    return b.value();
                }
                throw refused(node, c);
            }
            case "VARCHAR", "CHAR", "TEXT" -> {
                return text(node, c);
            }
            // the structured slots: each element or field by its own declared slot (the slot's spelling read back,
            // {@link #element}, {@link #fields}, {@link #mapSlots}; the legacy executor's unwrap by the declared layout)
            case "ARRAY" -> {
                if (node instanceof Json.Arr a) {
                    ExecutionPlan.Column element = element(c);
                    List<Object> out = new ArrayList<>(a.items().size());
                    for (Json.Node item : a.items()) {
                        out.add(cell(item, element));
                    }
                    return out;
                }
                throw refused(node, c);
            }
            case "STRUCT" -> {
                if (node instanceof Json.Obj o) {
                    Map<String, Object> out = new LinkedHashMap<>();
                    for (ExecutionPlan.Column f : fields(c)) {
                        Json.Node fv = o.fields().get(f.name());
                        if (fv == null && CellText.synthetic(f.name())) {
                            // an identity field the writer dropped (CellText.declared: an object is its declared
                            // properties) is not read back as a null property (2026-10-29, the twin's reader)
                            continue;
                        }
                        out.put(f.name(), fv == null ? null : cell(fv, f));
                    }
                    return out;
                }
                throw refused(node, c);
            }
            case "MAP" -> {
                ExecutionPlan.Column[] kv = mapSlots(c);
                Map<Object, Object> out = new LinkedHashMap<>();
                if (node instanceof Json.Obj o) {
                    o.fields().forEach((k, v) -> out.put(cell(new Json.Str(k), kv[0]), cell(v, kv[1])));
                    return out;
                }
                if (node instanceof Json.Arr a) {
                    // a map carried as its entries, each a {key, value} object
                    for (Json.Node entry : a.items()) {
                        if (!(entry instanceof Json.Obj e) || e.fields().size() != 2 || !e.fields().containsKey("key")
                                || !e.fields().containsKey("value")) {
                            throw new IllegalStateException("column '" + c.name() + "': a map cell's entry is not a"
                                    + " {key, value} object: " + excerpt(entry));
                        }
                        out.put(cell(e.fields().get("key"), kv[0]), cell(e.fields().get("value"), kv[1]));
                    }
                    return out;
                }
                throw refused(node, c);
            }
            default -> {
                // no slot, or a JSON one: the Pure type decides
            }
        }
        if (type.equals("Integer")) {
            return integer(node, c);
        }
        if (type.equals("Float")) {
            return floating(node, c);
        }
        if (type.equals("Decimal") || type.startsWith("Decimal(")) {
            return decimal(node, c);
        }
        if (type.equals("Number")) {
            return switch (node) {
                case Json.Num n when n.isInteger() -> (Object) n.longValue();
                case Json.Num n -> exactDecimal(n);
                default -> throw refused(node, c);
            };
        }
        if (type.equals("String")) {
            return text(node, c);
        }
        if (type.equals("Boolean")) {
            if (node instanceof Json.Bool b) {
                return b.value();
            }
            throw refused(node, c);
        }
        if (type.equals("StrictDate") || type.equals("DateTime") || type.equals("Date")) {
            return PureDateLiteral.parse(text(node, c).trim().replace(' ', 'T'));
        }
        if (type.equals("Time")) {
            return PureTimeLiteral.parse(text(node, c).trim());
        }
        // a class instance, an enumeration's value, Any: the token's own kind, as the legacy executor's Any lane
        // reads a JSON cell (a string, a boolean, a whole number as a long, any other number as a double, a
        // structure as its map or list)
        return switch (node) {
            case Json.Str s -> s.value();
            case Json.Bool b -> b.value();
            case Json.Num n -> n.isInteger() ? (Object) n.longValue() : (Object) n.doubleValue();
            case Json.Obj o -> plain(o);
            case Json.Arr a -> plain(a);
            case Json.Null nil -> null;
        };
    }

    /** The value-lane's decimal text ({@code DECIMAL_TEXT}: the canonical D-suffixed spelling) as its exact decimal. */
    public static BigDecimal decimalText(String s) {
        return new BigDecimal(s.endsWith("D") || s.endsWith("d") ? s.substring(0, s.length() - 1) : s);
    }

    private static final String ANY = "meta::pure::metamodel::type::Any";

    /** Whether the Pure type {@code type} is a number's: Integer, Float, Decimal (with or without its digits) or
     *  Number. */
    public static boolean numericPure(String type) {
        return type.equals("Integer") || type.equals("Float") || type.equals("Decimal") || type.startsWith("Decimal(")
                || type.equals("Number");
    }

    /** A structure as plain Java: an object an ordered map, an array a list, a whole number a long, any other
     *  number its exact decimal (the SQL layer's JSON reader's rule, which the legacy executor's structures took). */
    private static @com.legend.base.Nullable Object plain(Json.Node node) {
        return switch (node) {
            case Json.Obj o -> {
                Map<String, Object> m = new LinkedHashMap<>();
                o.fields().forEach((k, v) -> m.put(k, plain(v)));
                yield m;
            }
            case Json.Arr a -> {
                List<Object> l = new ArrayList<>(a.items().size());
                for (Json.Node item : a.items()) {
                    l.add(plain(item));
                }
                yield l;
            }
            case Json.Str s -> s.value();
            case Json.Bool b -> b.value();
            case Json.Num n -> n.isInteger() ? (Object) n.longValue() : (Object) exactDecimal(n);
            case Json.Null nil -> null;
        };
    }

    private static Object integer(Json.Node node, ExecutionPlan.Column c) {
        BigInteger whole = switch (node) {
            case Json.Num n when n.isInteger() -> BigInteger.valueOf(n.longValue());
            case Json.Num n when n.decimalValue() != null && n.decimalValue().stripTrailingZeros().scale() <= 0 ->
                    n.decimalValue().toBigIntegerExact();
            case Json.Str s -> new BigInteger(s.value().trim());
            default -> throw refused(node, c);
        };
        // a long holds a whole number of up to 63 bits beside its sign (bitLength counts the sign apart): until
        // 2026-10-29 the bound was 63 exclusive, and [2^62, 2^63-1] rode as a BigInteger (the track A audit, finding 2)
        return whole.bitLength() <= 63 ? (Object) whole.longValue() : whole;
    }

    private static Object floating(Json.Node node, ExecutionPlan.Column c) {
        double d = switch (node) {
            case Json.Num n -> n.doubleValue();
            case Json.Str s -> Double.valueOf(s.value().trim());   // a Float the wire spells as text (NaN, Infinity)
            default -> throw refused(node, c);
        };
        // a REAL (four bytes) its own shortest digits, as JdbcDecode reads the driver's float: a host that widens it to
        // a double (DuckDB-WASM's, Python's) hands 0.10000000149011612 for 0.1 (the phase 3 audit, 2026-10-27)
        return slotWord(c.slot()).equals("REAL") && Double.isFinite(d) ? Double.parseDouble(Float.toString((float) d))
                : d;
    }

    private static BigDecimal decimal(Json.Node node, ExecutionPlan.Column c) {
        return switch (node) {
            case Json.Num n -> exactDecimal(n);
            case Json.Str s -> new BigDecimal(s.value().trim());
            default -> throw refused(node, c);
        };
    }

    /** The number's exact decimal: its token's digits (the scale as written), never the double's. */
    private static BigDecimal exactDecimal(Json.Num n) {
        if (n.decimalValue() != null) {
            return n.decimalValue();
        }
        if (n.token() != null) {
            return new BigDecimal(n.token());
        }
        return n.isInteger() ? BigDecimal.valueOf(n.longValue()) : BigDecimal.valueOf(n.doubleValue());
    }

    private static String text(Json.Node node, ExecutionPlan.Column c) {
        if (node instanceof Json.Str s) {
            return s.value();
        }
        // a Decimal's token is written bare; a text-typed cell the database wrote as a number keeps its digits
        if (node instanceof Json.Num n && n.token() != null) {
            return n.token();
        }
        if (node instanceof Json.Num n) {
            return n.isInteger() ? Long.toString(n.longValue()) : exactDecimal(n).toPlainString();
        }
        throw refused(node, c);
    }

    /** A temporal carried as its text ({@code TEMPORAL_TEXT}): a time of day when the column's Pure type is
     *  {@code StrictTime}, a date otherwise, each at the precision written (a Cypher local time's text read as a date
     *  failed "expected '-' after year": 2026-10-27, the result boundary program's phase 5, the Cypher TCK's ordered
     *  local times, which the old executor handed over as text unread). */
    public static Object temporalText(String text, ExecutionPlan.Column c) {
        return c.type().equals("StrictTime") ? PureTimeLiteral.parse(text.trim())
                : PureDateLiteral.parse(text.trim().replace(' ', 'T'));
    }

    /** The slot's leading word: {@code DECIMAL} of {@code DECIMAL(10,2)}, {@code ARRAY} of {@code ARRAY<...>}. */
    public static String slotWord(@com.legend.base.Nullable String slot) {
        if (slot == null) {
            return "";
        }
        int end = slot.length();
        for (int i = 0; i < slot.length(); i++) {
            char ch = slot.charAt(i);
            if (ch == '(' || ch == '<' || ch == ' ') {
                end = i;
                break;
            }
        }
        return slot.substring(0, end);
    }

    // ------------------------------------------------------------------
    // the structured slots' spelling (ColumnFacts.slot writes it; both decodes read it back here)
    // ------------------------------------------------------------------

    /** The element column of a list column: the column's name and type, the element slot of {@code ARRAY<...>}. A
     *  column made for a nested value (this, a map's slots, a struct's fields, a tagged column's tag and slots) has
     *  no written-precision fact of its own: the decoders read none, and the writers spell a nested temporal by the
     *  value's own precision and the parent column's fact ({@code CellText.structure}). */
    public static ExecutionPlan.Column element(ExecutionPlan.Column c) {
        return new ExecutionPlan.Column(c.name(), c.type(), c.multiplicity(), inside(c, "ARRAY"), null, null);
    }

    /** The key and value columns of a map column ({@code MAP<key,value>}): each its slot, its type Any. */
    public static ExecutionPlan.Column[] mapSlots(ExecutionPlan.Column c) {
        List<String> parts = topLevel(inside(c, "MAP"));
        if (parts.size() != 2) {
            throw new IllegalStateException("column '" + c.name() + "': its slot " + c.slot() + " is no MAP<key,value>");
        }
        return new ExecutionPlan.Column[] {any("key", parts.get(0).trim()), any("value", parts.get(1).trim())};
    }

    /** A tagged slot's value as its kind presents it: a DateTime at the written precision the kind carries
     *  ({@code DateTime} the second, {@code DateTime(n)} n subsecond digits: {@link PureDateLiteral#presentedAt}),
     *  every other kind as read. */
    public static @com.legend.base.Nullable Object presented(@com.legend.base.Nullable Object v, ExecutionPlan.Column slot) {
        String kind = slot.type();
        if (!(v instanceof PureDateLiteral d) || !kind.startsWith("DateTime")) {
            return v;
        }
        int digits = kind.startsWith("DateTime(") ? Integer.parseInt(kind.substring("DateTime(".length(),
                kind.length() - 1)) : 0;
        return d.presentedAt(digits);
    }

    /** A tagged column's own cell, the tag: an integer, the slot index. */
    public static ExecutionPlan.Column tagOf(ExecutionPlan.Column c) {
        return new ExecutionPlan.Column(c.name(), "Integer", c.multiplicity(), "INTEGER", null, null);
    }

    /** The result columns a statement answers for the plan's {@code columns}: each its own, plus a tagged column's
     *  slot columns beside it (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a). */
    public static int physicalColumns(List<ExecutionPlan.Column> columns) {
        int n = 0;
        for (ExecutionPlan.Column c : columns) {
            n += slotWord(c.slot()).equals("TAGGED") ? 1 + topLevel(inside(c, "TAGGED")).size() : 1;
        }
        return n;
    }

    /** Slot {@code index} of a tagged column ({@code TAGGED<kind slot, ...>}, docs/RESULTS_AND_RENDERERS_2026_10_22.md
     *  §5a) as a column: the physical column {@link ExecutionPlan.Column#taggedSlotColumn} names, typed as the
     *  slot's kind, in the slot's SQL type. */
    public static ExecutionPlan.Column taggedSlot(ExecutionPlan.Column c, int index) {
        List<String> parts = topLevel(inside(c, "TAGGED"));
        if (index < 0 || index >= parts.size()) {
            throw new IllegalStateException("column '" + c.name() + "': its tag " + index + " names no slot of "
                    + c.slot());
        }
        String f = parts.get(index).trim();
        int space = f.indexOf(' ');
        if (space <= 0) {
            throw new IllegalStateException("column '" + c.name() + "': its slot " + c.slot()
                    + " has an entry with no type: '" + f + "'");
        }
        return new ExecutionPlan.Column(ExecutionPlan.Column.taggedSlotColumn(c.name(), index),
                f.substring(0, space), c.multiplicity(), f.substring(space + 1).trim(), null, null);
    }

    /** A tagged column's slots as columns, in order ({@link #taggedSlot}; the Arrow writer's union members, track A,
     *  2026-10-29). */
    public static List<ExecutionPlan.Column> taggedSlots(ExecutionPlan.Column c) {
        int n = topLevel(inside(c, "TAGGED")).size();
        List<ExecutionPlan.Column> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(taggedSlot(c, i));
        }
        return out;
    }

    /** The fields of a struct column ({@code STRUCT<name slot, ...>}) as columns: each its name and its slot, its
     *  type Any (a field declares no Pure type; the slot, then the carrier's own kind, decides). */
    public static List<ExecutionPlan.Column> fields(ExecutionPlan.Column c) {
        List<ExecutionPlan.Column> out = new ArrayList<>();
        for (String part : topLevel(inside(c, "STRUCT"))) {
            String f = part.trim();
            int space = f.indexOf(' ');
            if (space <= 0) {
                throw new IllegalStateException("column '" + c.name() + "': its slot " + c.slot()
                        + " has a field with no type: '" + f + "'");
            }
            out.add(any(f.substring(0, space), f.substring(space + 1).trim()));
        }
        return out;
    }

    /** A structure's field (or a map's key or value) as a column: it declares no Pure type of its own, so its slot
     *  is its only fact -- a numeric slot types it as that number (a host hands a nested decimal as its exact text,
     *  which the field reads as the decimal the server's driver reads; the phase 3 audit, 2026-10-27), any other is
     *  Any, the carrier's own kind deciding. */
    private static ExecutionPlan.Column any(String name, String slot) {
        String type = switch (slotWord(slot)) {
            case "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT" -> "Integer";
            case "DOUBLE", "FLOAT", "REAL" -> "Float";
            case "DECIMAL", "NUMERIC" -> "Decimal";
            default -> ANY;
        };
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(0, 1),
                slot, null, null);
    }

    private static String inside(ExecutionPlan.Column c, String word) {
        String slot = c.slot();
        if (slot == null || !slot.startsWith(word + "<") || !slot.endsWith(">")) {
            throw new IllegalStateException("column '" + c.name() + "': its slot " + slot + " is no " + word + "<...>");
        }
        return slot.substring(word.length() + 1, slot.length() - 1);
    }

    /** {@code s} split at its top-level commas (none inside {@code <...>} or {@code (...)}). */
    private static List<String> topLevel(String s) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        int from = 0;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == '<' || ch == '(') {
                depth++;
            } else if (ch == '>' || ch == ')') {
                depth--;
            } else if (ch == ',' && depth == 0) {
                out.add(s.substring(from, i));
                from = i + 1;
            }
        }
        out.add(s.substring(from));
        return out;
    }

    private static IllegalStateException refused(Json.Node node, ExecutionPlan.Column c) {
        return new IllegalStateException("column '" + c.name() + "' is declared " + c.type()
                + (c.slot() == null ? "" : " in slot " + c.slot()) + ", and its cell's token " + excerpt(node)
                + " is not of that kind");
    }

    private static String excerpt(Json.Node node) {
        return excerpt(Json.toCompact(node));
    }

    private static String excerpt(String s) {
        return s.length() > 120 ? s.substring(0, 120) + "..." : s;
    }
}
