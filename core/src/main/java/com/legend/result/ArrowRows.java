// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.arrowipc.ArrowField;
import com.legend.arrowipc.ArrowReader;
import com.legend.arrowipc.ArrowType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.result.RowDecode.Kind;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The rows a HOST's database answered a plan's statement with, handed over as ARROW (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md
 * R3, D8: the core's Arrow reader), decoded by the plan's own columns and streamed to a sink -- so a host's answer is
 * written by the writers legend-lite's server writes with, from the bytes its database answered, with no text between.
 * The twin of {@link HostRows} (the same rows as JSON text) and of the JDBC session's decode ({@code exec.JdbcDecode}):
 * the rules are that decode's, arm by arm -- the slot first ({@link Kind}, one table), the Pure type where the slot is
 * silent, the wire's own kind where both are -- and the carrier is the one each of them hands the batch (a date kind a
 * {@link PureDateLiteral}, a time a {@link PureTimeLiteral}, an Integer a {@code long} or past 63 bits a
 * {@link BigInteger}, a Float a {@code double}, a Decimal its exact {@link BigDecimal}, a structure its ordered map or
 * list). Each Arrow value is read as the Java value a JDBC driver hands for its SQL type (an integer of its width, a
 * decimal at its scale, a timestamp as its UTC wall clock, a zoned one as its offset date-time at UTC, a date, a time,
 * a list, a struct's attributes, a map), then decoded as that decode decodes it: nothing is computed, and a value no
 * arm reads is refused by name, never guessed (AGENTS.md invariant 4).
 *
 * <p>What is decided once per column, never per cell: its decode kind (from its slot) and how its Arrow type is read.
 * The stream's columns are the statement's, in order (as a JDBC result's are): the plan's i-th column is the stream's
 * i-th field, a tagged column's slots found beside them by name; a stream of another width is refused by name.
 *
 * <p>Where a JDBC driver's facts change a decode (a driver that drops a decimal's trailing zeros, one that writes a
 * non-finite JSON number, one that re-reads no timestamp), Arrow carries the value whole -- a decimal at its type's
 * scale, a timestamp at its unit -- so no such fact arises; a JSON text with a non-finite number is refused, as the
 * DuckDB session's decode refuses it.
 */
public final class ArrowRows {

    private ArrowRows() {
    }

    /** {@code arrow}, the Arrow IPC stream a host's database answered a statement whose result columns are
     *  {@code columns} with, streamed to {@code sink} in batches. {@code arrow}'s bytes are read in place. */
    public static void stream(ByteBuffer arrow, List<ExecutionPlan.Column> columns, ResultSink sink) throws IOException {
        ArrowReader.Stream in = ArrowReader.open(arrow);
        List<ArrowField> fields = in.schema().fields();
        int physical = RowDecode.physicalColumns(columns);
        if (fields.size() != physical) {
            throw new IllegalArgumentException("the host's Arrow answers " + fields.size() + " columns "
                    + fields.stream().map(ArrowField::name).toList() + "; the plan's statement answers " + physical
                    + " (" + columns.stream().map(ExecutionPlan.Column::name).toList() + ")");
        }
        Column[] readers = new Column[columns.size()];
        for (int i = 0; i < readers.length; i++) {
            ExecutionPlan.Column c = columns.get(i);
            readers[i] = RowDecode.slotWord(c.slot()).equals("TAGGED") ? tagged(c, i, fields)
                    : new Plain(i, decoder(c, fields.get(i)));
        }
        Batcher batcher = new Batcher(columns, sink, Batcher.DEFAULT_CAPACITY);
        batcher.start();
        for (ArrowReader.Batch b = in.next(); b != null; b = in.next()) {
            for (int r = 0; r < b.rows(); r++) {
                List<Object> cells = new ArrayList<>(readers.length);
                for (Column reader : readers) {
                    cells.add(reader.cell(b, r));
                }
                batcher.row(cells);
            }
        }
        batcher.end();
    }

    // -- a column's reader, decided once -----------------------------------------------

    /** One plan column's cell of a batch's row. */
    private sealed interface Column permits Plain, Tagged {
        @com.legend.base.Nullable Object cell(ArrowReader.Batch b, int row);
    }

    /** A column read from the stream's field {@code at}. */
    private record Plain(int at, Decoder decoder) implements Column {
        @Override
        public @com.legend.base.Nullable Object cell(ArrowReader.Batch b, int row) {
            return decoder.read(b.columns().get(at), row);
        }
    }

    /** A tagged column (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a): its field the tag, the slot index, read as the
     *  integer it is; the value in that slot's field beside it, read with the slot's own type -- the JDBC decode's
     *  {@code tagged} arm. */
    private record Tagged(ExecutionPlan.Column column, int at, Decoder tag, List<ExecutionPlan.Column> slots,
            int[] slotAt, List<Decoder> slotDecoders) implements Column {
        @Override
        public @com.legend.base.Nullable Object cell(ArrowReader.Batch b, int row) {
            Object t = tag.read(b.columns().get(at), row);
            if (t == null) {
                return null;
            }
            int index = ((Number) t).intValue();
            if (index < 0 || index >= slots.size()) {
                throw new IllegalStateException("the row's tagged column '" + column.name() + "' names slot " + index
                        + " of " + slots.size());
            }
            ExecutionPlan.Column slot = slots.get(index);
            Object value = slotDecoders.get(index).read(b.columns().get(slotAt[index]), row);
            if (value == null) {
                throw new IllegalStateException("the row's tagged column '" + column.name() + "' names slot "
                        + slot.name() + ", and that slot's column is null");
            }
            return new TaggedCell(index, java.util.Objects.requireNonNull(RowDecode.presented(value, slot)));
        }
    }

    private static Tagged tagged(ExecutionPlan.Column c, int at, List<ArrowField> fields) {
        List<ExecutionPlan.Column> slots = RowDecode.taggedSlots(c);
        int[] slotAt = new int[slots.size()];
        List<Decoder> decoders = new ArrayList<>(slots.size());
        for (int s = 0; s < slots.size(); s++) {
            String name = slots.get(s).name();
            int found = -1;
            for (int f = 0; f < fields.size(); f++) {
                if (fields.get(f).name().equals(name)) {
                    found = f;
                    break;
                }
            }
            if (found < 0) {
                throw new IllegalArgumentException("the host's Arrow has no column '" + name + "', the slot of tagged"
                        + " column '" + c.name() + "'");
            }
            slotAt[s] = found;
            decoders.add(decoder(slots.get(s), fields.get(found)));
        }
        return new Tagged(c, at, decoder(RowDecode.tagOf(c), fields.get(at)), slots, slotAt, decoders);
    }

    /** A column's non-null cells, decoded; a null cell is null. */
    @FunctionalInterface
    private interface Decoder {
        @com.legend.base.Nullable Object read(ArrowReader.Array a, int row);
    }

    private static Decoder decoder(ExecutionPlan.Column c, ArrowField f) {
        Kind kind = Kind.of(RowDecode.slotWord(c.slot()));
        if (kind == Kind.TAGGED) {
            throw new IllegalStateException("column '" + c.name() + "': a tagged cell is read with its row (its slot"
                    + " columns stand beside it), never alone");
        }
        Wire wire = wire(f);
        return (a, row) -> a.isNull(row) ? null : decode(wire.at(a, row), c, kind);
    }

    // -- an Arrow value as a driver hands it -------------------------------------------

    /** A non-null value of an array, as the Java value a JDBC driver hands for its SQL type. */
    @FunctionalInterface
    private interface Wire {
        Object at(ArrowReader.Array a, int i);
    }

    /** How {@code f}'s values are read, decided by its Arrow type once. */
    private static Wire wire(ArrowField f) {
        return switch (f.type()) {
            case ArrowType.Null n -> (a, i) -> {
                throw new IllegalStateException("a Null column's value read");
            };
            case ArrowType.Int t -> integer(t);
            case ArrowType.FloatingPoint t -> t.precision() == ArrowType.FloatPrecision.SINGLE
                    ? (a, i) -> (Object) a.buffer(0).getFloat(4 * i)
                    : (a, i) -> (Object) a.buffer(0).getDouble(8 * i);
            case ArrowType.Bool t -> (a, i) -> (a.buffer(0).get(i >>> 3) & (1 << (i & 7))) != 0;
            case ArrowType.Decimal t -> {
                int width = t.bits() / 8;
                int scale = t.scale();
                yield (a, i) -> {
                    // little-endian two's complement, read big-endian for BigInteger: the unscaled value at the type's scale
                    byte[] big = new byte[width];
                    ByteBuffer b = a.buffer(0);
                    for (int k = 0; k < width; k++) {
                        big[k] = b.get(width * i + width - 1 - k);
                    }
                    return new BigDecimal(new BigInteger(big), scale);
                };
            }
            case ArrowType.Utf8 t -> (a, i) -> utf8(a, i, false);
            case ArrowType.LargeUtf8 t -> (a, i) -> utf8(a, i, true);
            case ArrowType.Binary t -> (a, i) -> bytes(a, i, false);
            case ArrowType.LargeBinary t -> (a, i) -> bytes(a, i, true);
            case ArrowType.Date t -> t.unit() == ArrowType.DateUnit.DAY
                    ? (a, i) -> LocalDate.ofEpochDay(a.buffer(0).getInt(4 * i))
                    : (a, i) -> LocalDate.ofEpochDay(Math.floorDiv(a.buffer(0).getLong(8 * i), 86_400_000L));
            case ArrowType.Time t -> {
                long nanosPer = nanosPer(t.unit());
                yield t.bits() == 32
                        ? (a, i) -> LocalTime.ofNanoOfDay(a.buffer(0).getInt(4 * i) * nanosPer)
                        : (a, i) -> LocalTime.ofNanoOfDay(a.buffer(0).getLong(8 * i) * nanosPer);
            }
            case ArrowType.Timestamp t -> {
                long perSecond = 1_000_000_000L / nanosPer(t.unit());
                long nanosPer = nanosPer(t.unit());
                boolean zoned = t.timezone() != null;
                yield (a, i) -> {
                    long v = a.buffer(0).getLong(8 * i);
                    LocalDateTime utc = LocalDateTime.ofEpochSecond(Math.floorDiv(v, perSecond),
                            (int) (Math.floorMod(v, perSecond) * nanosPer), ZoneOffset.UTC);
                    // a zoned timestamp is an instant: its offset date-time at UTC, as a driver hands a TIMESTAMPTZ
                    return zoned ? (Object) OffsetDateTime.of(utc, ZoneOffset.UTC) : utc;
                };
            }
            case ArrowType.List t -> list(f, false);
            case ArrowType.FixedSizeList t -> list(f, true);
            case ArrowType.Struct t -> {
                List<Wire> kids = new ArrayList<>();
                for (ArrowField k : f.children()) {
                    kids.add(wire(k));
                }
                yield (a, i) -> {
                    @com.legend.base.Nullable Object[] attrs = new Object[kids.size()];
                    for (int k = 0; k < attrs.length; k++) {
                        ArrowReader.Array child = a.children().get(k);
                        attrs[k] = child.isNull(i) ? null : kids.get(k).at(child, i);
                    }
                    return attrs;
                };
            }
            case ArrowType.Map t -> {
                ArrowField entries = f.children().get(0);
                if (entries.children().size() != 2) {
                    throw new IllegalArgumentException("the Arrow map column '" + f.name() + "' has entries of "
                            + entries.children().size() + " fields, not a key and a value");
                }
                Wire key = wire(entries.children().get(0));
                Wire value = wire(entries.children().get(1));
                yield (a, i) -> {
                    ByteBuffer offsets = a.buffer(0);
                    ArrowReader.Array entry = a.children().get(0);
                    ArrowReader.Array keys = entry.children().get(0);
                    ArrowReader.Array values = entry.children().get(1);
                    Map<Object, Object> out = new LinkedHashMap<>();
                    for (int e = offsets.getInt(4 * i); e < offsets.getInt(4 * i + 4); e++) {
                        out.put(key.at(keys, e), values.isNull(e) ? null : value.at(values, e));
                    }
                    return out;
                };
            }
            case ArrowType.Interval t -> throw unread(f);
            case ArrowType.Union t -> throw unread(f);
        };
    }

    /** An integer of its width: a signed one of 8, 16 or 32 bits an {@code Integer}, 64 a {@code Long}; an unsigned one
     *  of 8 or 16 bits an {@code Integer}, 32 a {@code Long}, 64 a {@code Long} or past 63 bits a {@link BigInteger}. */
    private static Wire integer(ArrowType.Int t) {
        return switch (t.bits()) {
            case 8 -> t.signed() ? (a, i) -> (Object) (int) a.buffer(0).get(i)
                    : (a, i) -> (Object) (a.buffer(0).get(i) & 0xff);
            case 16 -> t.signed() ? (a, i) -> (Object) (int) a.buffer(0).getShort(2 * i)
                    : (a, i) -> (Object) (a.buffer(0).getShort(2 * i) & 0xffff);
            case 32 -> t.signed() ? (a, i) -> (Object) a.buffer(0).getInt(4 * i)
                    : (a, i) -> (Object) (a.buffer(0).getInt(4 * i) & 0xffff_ffffL);
            case 64 -> t.signed() ? (a, i) -> (Object) a.buffer(0).getLong(8 * i)
                    : (a, i) -> {
                        long v = a.buffer(0).getLong(8 * i);
                        return v >= 0 ? (Object) v : new BigInteger(Long.toUnsignedString(v));
                    };
            default -> throw new IllegalArgumentException("an Arrow integer of " + t.bits() + " bits");
        };
    }

    /** A list's values, each element as its own wire hands it (null where null). */
    private static Wire list(ArrowField f, boolean fixed) {
        if (f.children().size() != 1) {
            throw new IllegalArgumentException("the Arrow list column '" + f.name() + "' has "
                    + f.children().size() + " children");
        }
        Wire element = wire(f.children().get(0));
        int size = fixed ? ((ArrowType.FixedSizeList) f.type()).size() : 0;
        return (a, i) -> {
            ArrowReader.Array items = a.children().get(0);
            int from = fixed ? size * i : a.buffer(0).getInt(4 * i);
            int to = fixed ? from + size : a.buffer(0).getInt(4 * i + 4);
            List<Object> out = new ArrayList<>(to - from);
            for (int e = from; e < to; e++) {
                out.add(items.isNull(e) ? null : element.at(items, e));
            }
            return out;
        };
    }

    private static String utf8(ArrowReader.Array a, int i, boolean large) {
        return new String(bytes(a, i, large), StandardCharsets.UTF_8);
    }

    private static byte[] bytes(ArrowReader.Array a, int i, boolean large) {
        ByteBuffer offsets = a.buffer(0);
        int from = large ? Math.toIntExact(offsets.getLong(8 * i)) : offsets.getInt(4 * i);
        int to = large ? Math.toIntExact(offsets.getLong(8 * i + 8)) : offsets.getInt(4 * i + 4);
        byte[] out = new byte[to - from];
        a.buffer(1).get(from, out);
        return out;
    }

    private static long nanosPer(ArrowType.TimeUnit unit) {
        return switch (unit) {
            case SECOND -> 1_000_000_000L;
            case MILLISECOND -> 1_000_000L;
            case MICROSECOND -> 1_000L;
            case NANOSECOND -> 1L;
        };
    }

    private static IllegalArgumentException unread(ArrowField f) {
        return new IllegalArgumentException("the Arrow column '" + f.name() + "' is " + f.type() + ", which no "
                + "plan column reads");
    }

    // -- a driver's value as the plan's cell: the JDBC decode's arms --------------------

    /** {@code v}, a wire value (a cell, or a structure's element), as the cell of {@code c} of decode kind
     *  {@code kind}: by the slot first, then by the Pure type ({@code exec.JdbcDecode.decode}). */
    private static @com.legend.base.Nullable Object decode(@com.legend.base.Nullable Object v, ExecutionPlan.Column c,
            Kind kind) {
        if (v == null) {
            return null;
        }
        String type = c.type();
        switch (kind) {
            case TAGGED -> throw new IllegalStateException("column '" + c.name() + "': a tagged cell is read with"
                    + " its row (its slot columns stand beside it), never alone");
            case DECIMAL_TEXT -> {
                return RowDecode.decimalText(text(v, c));
            }
            case TEMPORAL_TEXT -> {
                return RowDecode.temporalText(text(v, c), c);
            }
            case TEMPORAL -> {
                return temporal(v, c);
            }
            case TIME -> {
                return time(v, c);
            }
            // a text the database holds under a numeric slot is the column's own text unless the Pure type is numeric
            case INTEGRAL -> {
                return v instanceof String s && !RowDecode.numericPure(type) ? s : integral(v, c);
            }
            case FLOATING -> {
                return v instanceof String s && !RowDecode.numericPure(type) ? s : floating(v, c);
            }
            case DECIMAL -> {
                return v instanceof String s && !RowDecode.numericPure(type) ? s : decimal(v, c);
            }
            case BOOLEAN -> {
                return bool(v, c);
            }
            case TEXT -> {
                return text(v, c);
            }
            case JSON -> {
                Json.Node node = Json.parse(jsonText(v, c));
                // a Variant's cell is its JSON text, a JSON null included (the SQL cell is not NULL)
                return type.equals(RowDecode.VARIANT) ? Json.toCompact(node) : RowDecode.cell(node, c);
            }
            case ARRAY -> {
                return array(v, c);
            }
            case STRUCT -> {
                return struct(v, c);
            }
            case MAP -> {
                return map(v, c);
            }
            case BY_PURE_TYPE -> {
                // no slot (a late-bound grid's column, a pivot's generated column): the Pure type decides
            }
        }
        return byPureType(v, c);
    }

    /** A timestamp as the engine reads one from a database (nine subsecond digits); a date a day; a text its written
     *  precision. */
    private static Object temporal(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case LocalDateTime t -> PureDateLiteral.fromDatabaseTimestamp(t);
            case OffsetDateTime o -> PureDateLiteral.fromDatabaseTimestamp(
                    o.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
            case LocalDate d -> PureDateLiteral.fromLocalDate(d);
            case String s -> PureDateLiteral.parse(s.trim().replace(' ', 'T'));
            default -> throw refused(v, c);
        };
    }

    private static Object time(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case LocalTime t -> PureTimeLiteral.fromLocalTime(t);
            case String s -> PureTimeLiteral.parse(s.trim());
            default -> throw refused(v, c);
        };
    }

    private static Object integral(Object v, ExecutionPlan.Column c) {
        BigInteger whole = switch (v) {
            case Long l -> BigInteger.valueOf(l);
            case Integer n -> BigInteger.valueOf(n);
            case BigInteger b -> b;
            // DuckDB answers a HUGEINT as a decimal of scale 0 (and a literal past int64 so)
            case BigDecimal d when d.stripTrailingZeros().scale() <= 0 -> d.toBigIntegerExact();
            case String s -> new BigInteger(s.trim());
            default -> throw refused(v, c);
        };
        // 63 bits beside the sign fit a long
        return whole.bitLength() <= 63 ? (Object) whole.longValue() : whole;
    }

    private static Object floating(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case Double d -> d;
            // a REAL's own digits, as the JSON writer's shortest text spelled them, never the widened binary
            case Float f -> Double.parseDouble(Float.toString(f));
            case BigDecimal d -> d.doubleValue();
            case Long l -> (double) l;
            case Integer n -> (double) n;
            case BigInteger b -> b.doubleValue();
            case String s -> Double.valueOf(s.trim());
            default -> throw refused(v, c);
        };
    }

    private static BigDecimal decimal(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case BigDecimal d -> d;
            case Long l -> BigDecimal.valueOf(l);
            case Integer n -> BigDecimal.valueOf(n);
            case BigInteger b -> new BigDecimal(b);
            case Double d -> BigDecimal.valueOf(d);
            case Float f -> new BigDecimal(Float.toString(f));
            case String s -> new BigDecimal(s.trim());
            default -> throw refused(v, c);
        };
    }

    private static Boolean bool(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case Boolean b -> b;
            // a database that answers a boolean as 1 or 0
            case Integer n -> n != 0;
            case Long n -> n != 0;
            default -> throw refused(v, c);
        };
    }

    /** A text slot's cell: the text, or a number's own digits where the database holds a number under it. */
    private static String text(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case String s -> s;
            case BigDecimal d -> d.toPlainString();
            case Long n -> Long.toString(n);
            case Integer n -> Integer.toString(n);
            case BigInteger b -> b.toString();
            case Double d -> Double.toString(d);
            case Float f -> Float.toString(f);
            default -> throw refused(v, c);
        };
    }

    /** A JSON cell's text: as the database hands it, or its UTF-8 bytes. */
    private static String jsonText(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case String s -> s;
            case byte[] b -> new String(b, StandardCharsets.UTF_8);
            default -> throw refused(v, c);
        };
    }

    private static Object array(Object v, ExecutionPlan.Column c) {
        ExecutionPlan.Column element = RowDecode.element(c);
        if (v instanceof List<?> items) {
            List<Object> out = new ArrayList<>(items.size());
            for (Object item : items) {
                out.add(decode(item, element, Kind.of(RowDecode.slotWord(element.slot()))));
            }
            return out;
        }
        // a list the database carries as JSON text, each element by the element slot
        if (v instanceof String || v instanceof byte[]) {
            Json.Node node = Json.parse(jsonText(v, c));
            if (node instanceof Json.Arr arr) {
                List<Object> out = new ArrayList<>(arr.items().size());
                for (Json.Node item : arr.items()) {
                    out.add(RowDecode.cell(item, element));
                }
                return out;
            }
        }
        throw refused(v, c);
    }

    private static @com.legend.base.Nullable Object struct(Object v, ExecutionPlan.Column c) {
        List<ExecutionPlan.Column> fields = RowDecode.fields(c);
        Map<String, Object> out = new LinkedHashMap<>();
        if (v instanceof Object[] attrs) {
            if (attrs.length != fields.size()) {
                throw new IllegalStateException("column '" + c.name() + "': a struct cell of " + attrs.length
                        + " attributes under a slot of " + fields.size() + " fields (" + c.slot() + ")");
            }
            for (int i = 0; i < attrs.length; i++) {
                ExecutionPlan.Column f = fields.get(i);
                out.put(f.name(), decode(attrs[i], f, Kind.of(RowDecode.slotWord(f.slot()))));
            }
            return out;
        }
        // a struct the database carries as JSON: its fields by the slot's declared layout; a JSON null is an absent
        // instance
        Json.Node node = Json.parse(jsonText(v, c));
        if (node instanceof Json.Null) {
            return null;
        }
        if (node instanceof Json.Obj o) {
            for (ExecutionPlan.Column f : fields) {
                Json.Node fv = o.fields().get(f.name());
                if (fv == null && CellText.synthetic(f.name())) {
                    continue;   // an identity field the JSON does not carry: not a null property (RowDecode's rule)
                }
                out.put(f.name(), fv == null ? null : RowDecode.cell(fv, f));
            }
            return out;
        }
        throw refused(v, c);
    }

    private static @com.legend.base.Nullable Object map(Object v, ExecutionPlan.Column c) {
        ExecutionPlan.Column[] kv = RowDecode.mapSlots(c);
        Kind keyKind = Kind.of(RowDecode.slotWord(kv[0].slot()));
        Kind valueKind = Kind.of(RowDecode.slotWord(kv[1].slot()));
        if (v instanceof Map<?, ?> m) {
            Map<Object, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                out.put(decode(e.getKey(), kv[0], keyKind), decode(e.getValue(), kv[1], valueKind));
            }
            return out;
        }
        // a map the database carries as JSON: its entries by the slot's key and value; a JSON null an absent map
        Json.Node node = Json.parse(jsonText(v, c));
        if (node instanceof Json.Null) {
            return null;
        }
        if (node instanceof Json.Obj) {
            return RowDecode.cell(node, c);
        }
        throw refused(v, c);
    }

    /** A cell with no slot: the Pure type decides, and where it is a class, an enumeration or Any, the value's own
     *  kind (a structure's JSON text parsed as a document's cell is). */
    private static @com.legend.base.Nullable Object byPureType(Object v, ExecutionPlan.Column c) {
        String type = c.type();
        if (type.equals(RowDecode.VARIANT)) {
            return jsonText(v, c);
        }
        if (type.equals("Integer")) {
            return integral(v, c);
        }
        if (type.equals("Float")) {
            return floating(v, c);
        }
        if (type.equals("Decimal") || type.startsWith("Decimal(")) {
            return decimal(v, c);
        }
        if (type.equals("Number")) {
            return switch (v) {
                case Long l -> l;
                case Integer n -> (long) n;
                case BigInteger b -> b.bitLength() <= 63 ? (Object) b.longValue() : b;
                case BigDecimal d -> d;
                case Double d -> d;
                case Float f -> Double.parseDouble(Float.toString(f));
                case String s -> s.contains(".") || s.contains("e") || s.contains("E") ? new BigDecimal(s.trim())
                        : integral(s, c);
                default -> throw refused(v, c);
            };
        }
        if (type.equals("String")) {
            return text(v, c);
        }
        if (type.equals("Boolean")) {
            return bool(v, c);
        }
        if (type.equals("StrictDate") || type.equals("DateTime") || type.equals("Date")) {
            return temporal(v, c);
        }
        if (type.equals("Time")) {
            return time(v, c);
        }
        // a class instance, an enumeration's value, Any: the value's own kind -- a structure's JSON text read as a
        // document's cell, a scalar as it is
        return switch (v) {
            case String s -> {
                String t = s.trim();
                yield t.startsWith("{") || t.startsWith("[") ? RowDecode.cell(Json.parse(t), c) : s;
            }
            case byte[] b -> RowDecode.cell(Json.parse(new String(b, StandardCharsets.UTF_8)), c);
            case Boolean b -> b;
            case Long l -> l;
            case Integer n -> (long) n;
            case BigInteger b -> b.bitLength() <= 63 ? (Object) b.longValue() : b;
            case BigDecimal d -> d;
            case Double d -> d;
            case Float f -> Double.parseDouble(Float.toString(f));
            case LocalDateTime t -> temporal(t, c);
            case LocalDate d -> temporal(d, c);
            case LocalTime t -> time(t, c);
            case List<?> items -> {
                List<Object> out = new ArrayList<>(items.size());
                for (Object item : items) {
                    out.add(decode(item, c, Kind.BY_PURE_TYPE));
                }
                yield out;
            }
            default -> throw refused(v, c);
        };
    }

    private static IllegalStateException refused(Object v, ExecutionPlan.Column c) {
        String text = v instanceof Object[] attrs ? java.util.Arrays.toString(attrs) : String.valueOf(v);
        return new IllegalStateException("column '" + c.name() + "' is declared " + c.type()
                + (c.slot() == null ? "" : " in slot " + c.slot()) + ", and the host's Arrow value, a "
                + v.getClass().getName() + " " + (text.length() > 120 ? text.substring(0, 120) + "..." : text)
                + ", is not of that kind");
    }
}
