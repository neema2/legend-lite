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
 * <p>Where a JDBC driver's facts change a decode, DuckDB's are read (the hosts' database; another database's facts
 * travel with it when a host reaches one, R7): a decimal and a timestamp arrive whole in Arrow, at the type's scale and
 * unit, so the trailing-zeros and re-read facts do not arise; a JSON cell's double that is not finite is DuckDB's bare
 * token, read by the JDBC decode's own parse ({@link NonFiniteJson}).
 *
 * <p>A text slot's cell is the driver's own text, as the JDBC session reads it ({@code getString}): a number's, a
 * boolean's or a day's text is its value object's {@code toString}, as DuckDB's driver hands it; a time of day's and a
 * timestamp's (DuckDB's own spelling) are refused by name. Where the two decodes are known to part (R3's audit,
 * docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md §7): a zoned time and an unsigned 128-bit integer, which DuckDB's Arrow
 * cannot carry whole, are refused by name, once per column; a HUGEINT under a column with no slot and a Pure type of
 * {@code Number} or {@code Any} is the decimal it arrives as (JDBC: the integer); a TINYINT under a Boolean, Decimal or
 * Number column and a UUID under a String one are read (JDBC's decode refuses the driver's {@code Byte} and
 * {@code UUID} there).
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
        List<ArrowReader.@com.legend.base.Nullable Encoding> encodings = in.schema().encodings();
        Column[] readers = new Column[columns.size()];
        for (int i = 0; i < readers.length; i++) {
            ExecutionPlan.Column c = columns.get(i);
            readers[i] = RowDecode.slotWord(c.slot()).equals("TAGGED") ? tagged(c, i, fields, encodings)
                    : column(c, i, fields.get(i), encodings.get(i));
        }
        // a column at a time (D20 (b)): each Arrow batch cut into batches of the writers' size, each column of each
        // filled by its reader straight into the batch's own arrays
        sink.columns(columns);
        for (ArrowReader.Batch b = in.next(); b != null; b = in.next()) {
            for (int from = 0; from < b.rows(); from += Batcher.DEFAULT_CAPACITY) {
                int n = Math.min(Batcher.DEFAULT_CAPACITY, b.rows() - from);
                Batch.Builder out = Batch.builder(columns, n);
                out.rows(n);
                for (int i = 0; i < readers.length; i++) {
                    readers[i].fill(b, from, n, out, i);
                }
                sink.batch(out.build());
            }
        }
        sink.end();
    }

    // -- a column's reader, decided once -----------------------------------------------

    /** One plan column's cells of a batch's rows {@code [from, from + n)}, filled into column {@code column} of
     *  {@code out} (its row 0 the batch's row {@code from}). */
    private sealed interface Column permits Plain, Tagged, Longs, Doubles, Booleans, Decimals, Texts {
        void fill(ArrowReader.Batch b, int from, int n, Batch.Builder out, int column);
    }

    /** A column read from the stream's field {@code at} a cell at a time: every arm of the decode (the definition the
     *  fast fills below each equal for the kinds and types they take). */
    private record Plain(int at, Decoder decoder) implements Column {
        @Override
        public void fill(ArrowReader.Batch b, int from, int n, Batch.Builder out, int column) {
            ArrowReader.Array a = b.columns().get(at);
            for (int r = 0; r < n; r++) {
                out.set(column, r, decoder.read(a, from + r));
            }
        }
    }

    /** An integer column read as {@code long}s: the decode's integral arm over an Arrow integer (an Integer, or a Number
     *  or a class with no slot, a long either way); an unsigned 64-bit value past 63 bits the BigInteger it is. */
    private record Longs(int at, ArrowType.Int t) implements Column {
        @Override
        public void fill(ArrowReader.Batch b, int from, int n, Batch.Builder out, int column) {
            ArrowReader.Array a = b.columns().get(at);
            ByteBuffer v = a.buffer(0);
            boolean signed = t.signed();
            for (int r = 0; r < n; r++) {
                int i = from + r;
                if (a.isNull(i)) {
                    out.setNull(column, r);
                    continue;
                }
                long x = switch (t.bits()) {
                    case 8 -> signed ? v.get(i) : v.get(i) & 0xff;
                    case 16 -> signed ? v.getShort(2 * i) : v.getShort(2 * i) & 0xffff;
                    case 32 -> signed ? v.getInt(4 * i) : v.getInt(4 * i) & 0xffff_ffffL;
                    default -> v.getLong(8 * i);
                };
                if (!signed && t.bits() == 64 && x < 0) {
                    out.set(column, r, new BigInteger(Long.toUnsignedString(x)));
                } else {
                    out.setLong(column, r, x);
                }
            }
        }
    }

    /** A double column: the decode's floating arm over an Arrow double, the double as it is. */
    private record Doubles(int at) implements Column {
        @Override
        public void fill(ArrowReader.Batch b, int from, int n, Batch.Builder out, int column) {
            ArrowReader.Array a = b.columns().get(at);
            ByteBuffer v = a.buffer(0);
            for (int r = 0; r < n; r++) {
                int i = from + r;
                if (a.isNull(i)) {
                    out.setNull(column, r);
                } else {
                    out.setDouble(column, r, v.getDouble(8 * i));
                }
            }
        }
    }

    /** A boolean column: the decode's boolean arm over an Arrow bit. */
    private record Booleans(int at) implements Column {
        @Override
        public void fill(ArrowReader.Batch b, int from, int n, Batch.Builder out, int column) {
            ArrowReader.Array a = b.columns().get(at);
            ByteBuffer v = a.buffer(0);
            for (int r = 0; r < n; r++) {
                int i = from + r;
                if (a.isNull(i)) {
                    out.setNull(column, r);
                } else {
                    out.setBoolean(column, r, (v.get(i >>> 3) & (1 << (i & 7))) != 0);
                }
            }
        }
    }

    /** A 128-bit decimal column: a value that fits 64 bits read as the long it is (its high word the sign of its low
     *  one) -- a Decimal its BigDecimal at the type's scale, an integral one (DuckDB's HUGEINT, scale 0) its long --
     *  every other value by the decode's own arm, the wider read. */
    private record Decimals(int at, int scale, boolean integral, Decoder wide) implements Column {
        @Override
        public void fill(ArrowReader.Batch b, int from, int n, Batch.Builder out, int column) {
            ArrowReader.Array a = b.columns().get(at);
            ByteBuffer v = a.buffer(0);
            for (int r = 0; r < n; r++) {
                int i = from + r;
                if (a.isNull(i)) {
                    out.setNull(column, r);
                    continue;
                }
                long low = v.getLong(16 * i);
                long high = v.getLong(16 * i + 8);
                if (high != (low >> 63)) {
                    out.set(column, r, wide.read(a, i));
                } else if (integral) {
                    out.setLong(column, r, low);
                } else {
                    out.set(column, r, BigDecimal.valueOf(low, scale));
                }
            }
        }
    }

    /** A text column: the decode's text arms over Arrow UTF-8, each cell its string, read from the buffer it lies in. */
    private record Texts(int at, boolean large) implements Column {
        @Override
        public void fill(ArrowReader.Batch b, int from, int n, Batch.Builder out, int column) {
            ArrowReader.Array a = b.columns().get(at);
            ByteBuffer offsets = a.buffer(0);
            ByteBuffer data = a.buffer(1);
            byte[] array = data.hasArray() ? data.array() : null;
            int base = data.hasArray() ? data.arrayOffset() : 0;
            for (int r = 0; r < n; r++) {
                int i = from + r;
                if (a.isNull(i)) {
                    out.setNull(column, r);
                    continue;
                }
                int start = large ? Math.toIntExact(offsets.getLong(8 * i)) : offsets.getInt(4 * i);
                int end = large ? Math.toIntExact(offsets.getLong(8 * i + 8)) : offsets.getInt(4 * i + 4);
                if (array != null) {
                    out.set(column, r, new String(array, base + start, end - start, StandardCharsets.UTF_8));
                } else {
                    byte[] bytes = new byte[end - start];
                    data.get(start, bytes);
                    out.set(column, r, new String(bytes, StandardCharsets.UTF_8));
                }
            }
        }
    }

    /**
     * Column {@code c}'s reader of the field {@code f} at {@code at}, decided once: a fast fill where the column's decode
     * kind and Arrow type have one (an integer read as longs, a double, a boolean, a 128-bit decimal, a text), the cell
     * decode otherwise -- each fast fill the decode's own arm for those kinds and types, nothing else.
     */
    private static Column column(ExecutionPlan.Column c, int at, ArrowField f,
            ArrowReader.@com.legend.base.Nullable Encoding encoding) {
        Decoder each = decoder(c, f, encoding);
        if (encoding != null) {
            return new Plain(at, each);
        }
        Kind kind = Kind.of(RowDecode.slotWord(c.slot()));
        String type = c.type();
        boolean byType = kind == Kind.BY_PURE_TYPE;
        return switch (f.type()) {
            case ArrowType.Int t when (kind == Kind.INTEGRAL || byType && (type.equals("Integer") || type.equals("Number")))
                    && (t.signed() || t.bits() <= 64) -> new Longs(at, t);
            case ArrowType.FloatingPoint t when t.precision() == ArrowType.FloatPrecision.DOUBLE
                    && (kind == Kind.FLOATING || byType && type.equals("Float")) -> new Doubles(at);
            case ArrowType.Bool t when kind == Kind.BOOLEAN || byType && type.equals("Boolean") -> new Booleans(at);
            case ArrowType.Decimal t when t.bits() == 128 && (kind == Kind.DECIMAL
                    || byType && (type.equals("Decimal") || type.startsWith("Decimal("))) -> new Decimals(at, t.scale(), false, each);
            case ArrowType.Decimal t when t.bits() == 128 && t.scale() == 0
                    && (kind == Kind.INTEGRAL || byType && type.equals("Integer")) -> new Decimals(at, 0, true, each);
            case ArrowType.Utf8 t when kind == Kind.TEXT || byType && type.equals("String") -> new Texts(at, false);
            case ArrowType.LargeUtf8 t when kind == Kind.TEXT || byType && type.equals("String") -> new Texts(at, true);
            default -> new Plain(at, each);
        };
    }

    /** A tagged column (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a): its field the tag, the slot index, read as the
     *  integer it is; the value in that slot's field beside it, read with the slot's own type -- the JDBC decode's
     *  {@code tagged} arm. */
    private record Tagged(ExecutionPlan.Column column, int at, Decoder tag, List<ExecutionPlan.Column> slots,
            int[] slotAt, List<Decoder> slotDecoders) implements Column {
        @Override
        public void fill(ArrowReader.Batch b, int from, int n, Batch.Builder out, int c) {
            for (int r = 0; r < n; r++) {
                out.set(c, r, cell(b, from + r));
            }
        }

        @com.legend.base.Nullable Object cell(ArrowReader.Batch b, int row) {
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

    private static Tagged tagged(ExecutionPlan.Column c, int at, List<ArrowField> fields,
            List<ArrowReader.@com.legend.base.Nullable Encoding> encodings) {
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
            decoders.add(decoder(slots.get(s), fields.get(found), encodings.get(found)));
        }
        return new Tagged(c, at, decoder(RowDecode.tagOf(c), fields.get(at), encodings.get(at)), slots, slotAt,
                decoders);
    }

    /** A column's non-null cells, decoded; a null cell is null. */
    @FunctionalInterface
    private interface Decoder {
        @com.legend.base.Nullable Object read(ArrowReader.Array a, int row);
    }

    /** {@code c}'s cells read from the field {@code f}; through its dictionary where {@code encoding} names one (a
     *  dictionary's null value a null cell, as a null index is). */
    private static Decoder decoder(ExecutionPlan.Column c, ArrowField f, ArrowReader.@com.legend.base.Nullable Encoding encoding) {
        Kind kind = Kind.of(RowDecode.slotWord(c.slot()));
        if (kind == Kind.TAGGED) {
            throw new IllegalStateException("column '" + c.name() + "': a tagged cell is read with its row (its slot"
                    + " columns stand beside it), never alone");
        }
        String word = RowDecode.slotWord(c.slot());
        String slot = c.slot() == null ? "" : c.slot().toUpperCase(java.util.Locale.ROOT);
        if (word.equals("UHUGEINT")) {
            // DuckDB's Arrow carries an unsigned 128-bit integer as a signed 128-bit decimal, its values past 2^127
            // wrapped (R3's audit): read whole only through the JDBC session
            throw new IllegalArgumentException("column '" + c.name() + "' is an unsigned 128-bit integer (" + c.slot()
                    + "), which the host's Arrow carries as " + f.type() + " with its largest values wrapped: refused");
        }
        if ((word.equals("TIMETZ") || slot.startsWith("TIME WITH TIME ZONE")) && f.type() instanceof ArrowType.Time) {
            // a zoned time's offset is dropped by DuckDB's Arrow (R3's audit); the JDBC session refuses its OffsetTime
            throw new IllegalArgumentException("column '" + c.name() + "' is a time with a zone (" + c.slot()
                    + "), whose offset the host's Arrow drops: refused");
        }
        Wire wire = wire(f);
        if (encoding != null) {
            return (a, row) -> {
                if (a.isNull(row)) {
                    return null;
                }
                ArrowReader.Array values = java.util.Objects.requireNonNull(a.dictionary());
                int k = a.index(row);
                return values.isNull(k) ? null : decode(wire.at(values, k), c, kind);
            };
        }
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
                    : (a, i) -> {
                        // a day of milliseconds: a whole number of days, never a time of day dropped (R3's audit)
                        long ms = a.buffer(0).getLong(8 * i);
                        if (Math.floorMod(ms, 86_400_000L) != 0) {
                            throw new IllegalStateException("the Arrow date column '" + f.name() + "' holds " + ms
                                    + " milliseconds, not a whole day");
                        }
                        return LocalDate.ofEpochDay(Math.floorDiv(ms, 86_400_000L));
                    };
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
            // the slots whose cells the JDBC session reads as the driver's own text (getString): that text
            case DECIMAL_TEXT -> {
                return RowDecode.decimalText(driverText(v, c));
            }
            case TEMPORAL_TEXT -> {
                return RowDecode.temporalText(driverText(v, c), c);
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
                return driverText(v, c);
            }
            case JSON -> {
                // as DuckDB writes it (its session's decoding fact, nonFiniteJson): a double that is not finite a bare
                // token, read by the JDBC decode's own parse (R3's audit)
                Json.Node node = NonFiniteJson.parse(jsonText(v, c));
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

    /**
     * A text slot's cell as the JDBC session reads it, the driver's own text ({@code getString}; R3's audit): a text
     * as it is; a number's, a boolean's or a day's the text DuckDB's JDBC driver hands, which is its value object's
     * {@code toString} (measured 2026-11-03 on duckdb_jdbc 1.5.6.0: a decimal's {@code 1E-10}, a double's
     * {@code 1.0E-10}, a REAL's {@code 0.1}); any other value (a time of day, a timestamp, whose text is DuckDB's own)
     * refused by name.
     */
    private static String driverText(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case String s -> s;
            case Integer n -> n.toString();
            case Long n -> n.toString();
            case BigInteger n -> n.toString();
            case BigDecimal d -> d.toString();
            case Double d -> d.toString();
            case Float f -> f.toString();
            case Boolean b -> b.toString();
            case LocalDate d -> d.toString();
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
