// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.arrowipc.ArrowField;
import com.legend.arrowipc.ArrowIpc;
import com.legend.arrowipc.ArrowType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * Lite's own Arrow answer, written at the edge (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R3, R8, R9; the result
 * boundary program's track A, 2026-10-29): the result as ONE Arrow IPC stream -- the schema from the plan's columns,
 * each batch a record batch, the end marker -- framed by the warehouse's own framer ({@code com.legend.arrowipc}), no
 * Arrow library. A column's Arrow type is its SLOT's (the database type the plan declares), as legend-engine's Arrow
 * answer types a column from its JDBC metadata (docs/SEMANTICS_REGISTER.md S28; the four divergences from S28's
 * format are register S125, pending the user's ruling): an Integer slot int64, a HUGEINT
 * decimal128(38, 0), a Float float64, {@code DECIMAL(p,s)} decimal128(p, s) at exactly that scale (a cell with more
 * digits is refused, never rounded: S28's defect is not copied), a Decimal of no declared scale its plain digits as
 * text, a StrictDate date32, a time-bearing column timestamp[ns, UTC] (lite's DateTime holds nine digits:
 * {@link ExecutionPlan.Precision#INSTANT}), a WRITTEN temporal (a literal, a partial date) Pure's text, a time
 * time64[ns], a list a List, a struct a Struct, a map a Map, a tagged column a dense Union of its slots, a JSON-carried
 * value its text. The plan's facts per column ride as the field's metadata ({@code legend.type}, {@code legend.slot},
 * {@code legend.precision}, {@code legend.multiplicity}); the stream's own metadata is the caller's (the engine's
 * {@code legend.builder} and the rest for the {@code ARROW_IPC} answer). A cell that is not its column's carrier is
 * refused by name; nothing here converts a value: an epoch day, epoch nanoseconds and a decimal's unscaled digits at
 * the column's scale are Arrow's representation of the value held whole.
 */
public final class ArrowWriter implements ResultSink {

    private final OutputStream out;
    private final java.util.function.Function<List<ExecutionPlan.Column>, Map<String, String>> metadata;
    private List<Enc> encoders = List.of();

    /** Onto {@code out}; {@code schemaMetadata} the stream's custom metadata (key to value), in order, made from the
     *  columns the result announces (an envelope of a dynamic pivot's columns is known only then). */
    public ArrowWriter(OutputStream out,
            java.util.function.Function<List<ExecutionPlan.Column>, Map<String, String>> schemaMetadata) {
        this.out = out;
        this.metadata = schemaMetadata;
    }

    /** Onto {@code out} with a fixed metadata. */
    public ArrowWriter(OutputStream out, Map<String, String> schemaMetadata) {
        this(out, columns -> schemaMetadata);
    }

    @Override
    public void columns(List<ExecutionPlan.Column> columns) throws IOException {
        List<Enc> encs = new ArrayList<>(columns.size());
        List<ArrowField> fields = new ArrayList<>(columns.size());
        for (ExecutionPlan.Column c : columns) {
            Enc e = encoder(c, c.name(), facts(c));
            encs.add(e);
            fields.add(e.field());
        }
        encoders = encs;
        out.write(ArrowIpc.schema(fields, new LinkedHashMap<>(metadata.apply(columns))));
    }

    @Override
    public void batch(Batch batch) throws IOException {
        ArrowIpc.Layout layout = new ArrowIpc.Layout();
        int n = batch.size();
        for (int i = 0; i < encoders.size(); i++) {
            int column = i;
            Enc enc = encoders.get(i);
            if (enc instanceof UnionOf u) {
                u.layout(n, row -> batch.get(row, column), row -> batch.tag(row, column), layout);
            } else {
                enc.layout(n, row -> batch.get(row, column), layout);
            }
        }
        out.write(ArrowIpc.recordBatch(n, layout, Map.of()));
        out.flush();
    }

    @Override
    public void end() throws IOException {
        out.write(ArrowIpc.END);
        out.flush();
    }

    /** The plan's facts of a column as its field's metadata. */
    static Map<String, String> facts(ExecutionPlan.Column c) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("legend.type", c.type());
        m.put("legend.multiplicity", pureMultiplicity(c.multiplicity()));
        if (c.slot() != null) {
            m.put("legend.slot", c.slot());
        }
        if (c.precision() != null) {
            m.put("legend.precision", c.precision().name());
        }
        return m;
    }

    /** Pure's spelling of a multiplicity ({@code [1]}, {@code [0..1]}, {@code [*]}, {@code [1..*]}; the track A
     *  audit's finding 13: until then lite's own {@code 0..1}). */
    private static String pureMultiplicity(ExecutionPlan.Multiplicity m) {
        Integer upper = m.upper();
        if (upper == null) {
            return m.lower() == 0 ? "[*]" : "[" + m.lower() + "..*]";
        }
        return m.lower() == upper ? "[" + upper + "]" : "[" + m.lower() + ".." + upper + "]";
    }

    // ---- the column's encoder from its facts --------------------------------------------------------------------

    /** The slot words whose carrier is a number -- unless the Pure type is not a number's, when the decoders carry
     *  the column's own text ({@link RowDecode#numericPure}). */
    private static final java.util.Set<String> NUMERIC_SLOTS = java.util.Set.of("BIGINT", "INTEGER", "SMALLINT",
            "TINYINT", "HUGEINT", "DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC");

    private static Enc encoder(ExecutionPlan.Column c, String name, Map<String, String> metadata) {
        String word = RowDecode.slotWord(c.slot());
        if (NUMERIC_SLOTS.contains(word) && !RowDecode.numericPure(c.type())) {
            // the decoders' rule (RowDecode.cell, JdbcDecode.cell): a non-numeric Pure type over a numeric slot (a
            // String-typed property over an INT column, measured on the corpus 2026-10-18) carries the column's own
            // text -- the type from the plan's two facts, never a fallback (the track A audit, finding 3)
            return new Text(name, metadata, c, Text.Form.TEXT);
        }
        return switch (word) {
            case "BIGINT", "INTEGER", "SMALLINT", "TINYINT" -> new Int64(name, metadata, c);
            case "HUGEINT" -> new Decimal128(name, metadata, c, 38, 0);
            case "DOUBLE", "FLOAT", "REAL" -> new Float64(name, metadata, c);
            case "DECIMAL", "NUMERIC" -> {
                int[] ps = precisionAndScale(c.slot());
                yield ps == null ? new Text(name, metadata, c, Text.Form.DIGITS)
                        : new Decimal128(name, metadata, c, ps[0], ps[1]);
            }
            case "DECIMAL_TEXT" -> new Text(name, metadata, c, Text.Form.DIGITS);
            case "BOOLEAN" -> new Bools(name, metadata, c);
            case "VARCHAR", "CHAR", "TEXT" -> new Text(name, metadata, c, Text.Form.TEXT);
            case "DATE" -> new Date32(name, metadata, c);
            case "TIMESTAMP", "TIMESTAMPTZ" -> new TimestampNanos(name, metadata, c);
            case "TEMPORAL_TEXT" -> new Text(name, metadata, c, Text.Form.PURE);
            case "TIME" -> new Time64(name, metadata, c);
            case "ARRAY" -> new ListOf(name, metadata, c, encoder(RowDecode.element(c), "item", Map.of()));
            case "STRUCT" -> {
                List<Enc> fields = new ArrayList<>();
                for (ExecutionPlan.Column f : RowDecode.fields(c)) {
                    fields.add(encoder(f, f.name(), Map.of()));
                }
                yield new StructOf(name, metadata, c, fields);
            }
            case "MAP" -> {
                ExecutionPlan.Column[] kv = RowDecode.mapSlots(c);
                yield new MapOf(name, metadata, c, encoder(kv[0], "key", Map.of()), encoder(kv[1], "value", Map.of()));
            }
            case "TAGGED" -> {
                List<ExecutionPlan.Column> slots = RowDecode.taggedSlots(c);
                List<Enc> members = new ArrayList<>(slots.size());
                for (ExecutionPlan.Column s : slots) {
                    members.add(encoder(s, s.type(), Map.of("legend.kind", s.type())));
                }
                yield new UnionOf(name, metadata, c, members);
            }
            default -> byPureType(c, name, metadata);
        };
    }

    /** A column of no slot, or a JSON one: the Pure type decides, as the decoder's own rule
     *  ({@link RowDecode#cell}); a variant's value is the JSON text the database built, carried as it is; a class's
     *  (or Any's: a struct's field, a map's slot) is the object the decoder read, written as the JSON route writes it
     *  ({@link CellText#pureJson}, compact) -- Arrow has no object type (the track A audit, finding 11). */
    private static Enc byPureType(ExecutionPlan.Column c, String name, Map<String, String> metadata) {
        String type = c.type();
        if (type.equals("Integer")) {
            return new Int64(name, metadata, c);
        }
        if (type.equals("Float")) {
            return new Float64(name, metadata, c);
        }
        if (type.equals("Decimal") || type.startsWith("Decimal(")) {
            return new Text(name, metadata, c, Text.Form.DIGITS);
        }
        if (type.equals("Boolean")) {
            return new Bools(name, metadata, c);
        }
        if (type.equals("String")) {
            return new Text(name, metadata, c, Text.Form.TEXT);
        }
        if (type.equals("StrictDate")) {
            return new Date32(name, metadata, c);
        }
        if (type.equals("DateTime") || type.startsWith("DateTime(")) {
            return new TimestampNanos(name, metadata, c);
        }
        if (type.equals("Date")) {
            return new Text(name, metadata, c, Text.Form.PURE);
        }
        if (type.equals("StrictTime")) {
            return new Time64(name, metadata, c);
        }
        if (type.equals(RowDecode.VARIANT)) {
            return new Text(name, metadata, c, Text.Form.TEXT);
        }
        if (type.contains("::")) {
            return new Text(name, metadata, c, Text.Form.JSON);
        }
        throw new com.legend.error.NotImplementedException("column '" + c.name() + "' (" + type + ", slot "
                + c.slot() + "): no Arrow type for it yet");
    }

    /** {@code DECIMAL(p,s)}'s p and s ({@code DECIMAL(p)} SQL's scale of zero), or null for a bare {@code DECIMAL}. */
    private static int @com.legend.base.Nullable [] precisionAndScale(@com.legend.base.Nullable String slot) {
        if (slot == null) {
            return null;
        }
        int open = slot.indexOf('('), close = slot.indexOf(')', open);
        if (open < 0 || close < 0) {
            return null;
        }
        int comma = slot.indexOf(',', open);
        boolean scaled = comma >= 0 && comma < close;
        String p = slot.substring(open + 1, scaled ? comma : close).strip();
        String s = scaled ? slot.substring(comma + 1, close).strip() : "0";
        return new int[] {Integer.parseInt(p), Integer.parseInt(s)};
    }

    private static IllegalStateException refused(ExecutionPlan.Column c, Object cell, String arrow) {
        return new IllegalStateException("column '" + c.name() + "' (" + c.type() + ", slot " + c.slot() + "): a cell "
                + cell.getClass().getSimpleName() + " " + cell + " is not what " + arrow + " holds");
    }

    // ---- the encoders: one per Arrow type this writer emits -----------------------------------------------------

    /** One array's encoder: its field, and the layout of {@code n} values read by {@code cells} (null for a null). */
    private abstract static sealed class Enc permits Int64, Float64, Bools, Decimal128, Text, Date32, TimestampNanos,
            Time64, ListOf, StructOf, MapOf, UnionOf {
        final ArrowField field;
        final ExecutionPlan.Column column;

        Enc(ArrowField field, ExecutionPlan.Column column) {
            this.field = field;
            this.column = column;
        }

        ArrowField field() {
            return field;
        }

        abstract void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out);

        /** The node and the validity buffer of {@code n} values. */
        static void nullable(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            int nulls = 0;
            for (int i = 0; i < n; i++) {
                if (cells.apply(i) == null) {
                    nulls++;
                }
            }
            out.node(n, nulls);
            out.buffer(ArrowIpc.validity(n, i -> cells.apply(i) == null));
        }
    }

    private static final class Int64 extends Enc {
        Int64(String name, Map<String, String> metadata, ExecutionPlan.Column c) {
            super(new ArrowField(name, new ArrowType.Int(64, true), true, List.of(), metadata), c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            long[] v = new long[n];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell instanceof Long l) {
                    v[i] = l;
                } else if (cell != null) {
                    throw refused(column, cell, "an int64");
                }
            }
            out.buffer(ArrowIpc.int64s(v, n));
        }
    }

    private static final class Float64 extends Enc {
        Float64(String name, Map<String, String> metadata, ExecutionPlan.Column c) {
            super(new ArrowField(name, new ArrowType.FloatingPoint(ArrowType.FloatPrecision.DOUBLE), true, List.of(),
                    metadata), c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            double[] v = new double[n];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell instanceof Double d) {
                    v[i] = d;
                } else if (cell != null) {
                    throw refused(column, cell, "a float64");
                }
            }
            out.buffer(ArrowIpc.float64s(v, n));
        }
    }

    private static final class Bools extends Enc {
        Bools(String name, Map<String, String> metadata, ExecutionPlan.Column c) {
            super(new ArrowField(name, new ArrowType.Bool(), true, List.of(), metadata), c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell != null && !(cell instanceof Boolean)) {
                    throw refused(column, cell, "a bool");
                }
            }
            out.buffer(ArrowIpc.bits(n, i -> Boolean.TRUE.equals(cells.apply(i))));
        }
    }

    /** decimal128(p, s): a cell's unscaled digits AT THE COLUMN'S SCALE -- a cell written at fewer places gets zeros
     *  (the same value), a cell with more is refused by name: never rounded. */
    private static final class Decimal128 extends Enc {
        private final int precision;
        private final int scale;

        Decimal128(String name, Map<String, String> metadata, ExecutionPlan.Column c, int precision, int scale) {
            super(new ArrowField(name, new ArrowType.Decimal(precision, scale, 128), true, List.of(), metadata), c);
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            BigInteger[] v = new BigInteger[n];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell == null) {
                    continue;
                }
                BigInteger unscaled = switch (cell) {
                    case BigDecimal d -> {
                        if (d.scale() > scale) {
                            throw new IllegalStateException("column '" + column.name() + "' (" + column.type()
                                    + ", slot " + column.slot() + "): the cell " + d.toPlainString() + " has more than "
                                    + scale + " decimal places, which decimal128(" + precision + ", " + scale
                                    + ") cannot hold: a value is never rounded");
                        }
                        yield (d.scale() == scale ? d : d.setScale(scale)).unscaledValue();
                    }
                    case BigInteger b when scale == 0 -> b;
                    case Long l when scale == 0 -> BigInteger.valueOf(l);
                    default -> throw refused(column, cell, "a decimal128");
                };
                if (unscaled.bitLength() > 127 || digitCount(unscaled) > precision) {
                    throw new IllegalStateException("column '" + column.name() + "' (" + column.type() + ", slot "
                            + column.slot() + "): the cell " + cell + " has more digits than decimal128(" + precision
                            + ", " + scale + ") holds");
                }
                v[i] = unscaled;
            }
            out.buffer(ArrowIpc.int128s(v, n));
        }
    }

    /** utf8: a text as it is; a decimal's plain digits; Pure's text of a cell ({@link CellText#pure}: a written
     *  temporal, a partial date); or an object's compact JSON as the JSON route writes it ({@link CellText#pureJson}). */
    private static final class Text extends Enc {
        enum Form { TEXT, DIGITS, PURE, JSON }

        private final Form form;

        Text(String name, Map<String, String> metadata, ExecutionPlan.Column c, Form form) {
            super(new ArrowField(name, new ArrowType.Utf8(), true, List.of(), metadata), c);
            this.form = form;
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            byte[][] v = new byte[n][];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell == null) {
                    continue;
                }
                String text = switch (form) {
                    case TEXT -> cell instanceof String s ? s : refusedText(cell, "a text");
                    case DIGITS -> switch (cell) {
                        case BigDecimal d -> d.toPlainString();
                        case BigInteger b -> b.toString();
                        case Long l -> Long.toString(l);
                        default -> refusedText(cell, "a decimal's digits");
                    };
                    case PURE -> CellText.pure(cell, column);
                    case JSON -> Json.toCompact(CellText.pureJson(cell, column));
                };
                v[i] = text.getBytes(StandardCharsets.UTF_8);
            }
            ArrowIpc.Varying varying = ArrowIpc.varying(v, n);
            out.buffer(varying.offsets());
            out.buffer(varying.data());
        }

        private String refusedText(Object cell, String arrow) {
            throw refused(column, cell, arrow);
        }
    }

    /** How many digits a whole number's text has, its sign aside (a decimal128's fit check). */
    private static int digitCount(BigInteger b) {
        String text = b.toString();
        return text.startsWith("-") ? text.length() - 1 : text.length();
    }

    /** A written subsecond's nine nanosecond digits: fewer are zero-padded (the same instant); more are refused by
     *  name -- a value is never truncated (the JSON route's INSTANT spelling drops a tenth digit: a finding of the
     *  track A audit, the homework README §9). */
    private static String nanosDigits(ExecutionPlan.Column c, String subsecond) {
        if (subsecond.length() > 9) {
            throw new IllegalStateException("column '" + c.name() + "' (" + c.type() + ", slot " + c.slot()
                    + "): a subsecond of " + subsecond.length() + " digits (." + subsecond
                    + ") does not fit a nanosecond timestamp, and a value is never truncated");
        }
        return (subsecond + "000000000").substring(0, 9);
    }

    /** date32: a day as days since 1970-01-01 (Arrow's representation of a day). */
    private static final class Date32 extends Enc {
        Date32(String name, Map<String, String> metadata, ExecutionPlan.Column c) {
            super(new ArrowField(name, new ArrowType.Date(ArrowType.DateUnit.DAY), true, List.of(), metadata), c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            int[] v = new int[n];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell instanceof PureDateLiteral.StrictDate d) {
                    v[i] = Math.toIntExact(java.time.LocalDate.of(d.year(), d.month(), d.day()).toEpochDay());
                } else if (cell != null) {
                    throw refused(column, cell, "a date32 (a day)");
                }
            }
            out.buffer(ArrowIpc.int32s(v, n));
        }
    }

    /** timestamp[ns, UTC]: a time-bearing date as nanoseconds since the epoch (Arrow's representation of the instant;
     *  lite's DateTime holds nine digits). A date of no time of day is refused by name (nothing here invents
     *  midnight); an instant outside the 64-bit nanosecond range (years before 1678 or after 2261) is refused by
     *  name. */
    private static final class TimestampNanos extends Enc {
        TimestampNanos(String name, Map<String, String> metadata, ExecutionPlan.Column c) {
            super(new ArrowField(name, new ArrowType.Timestamp(ArrowType.TimeUnit.NANOSECOND, "UTC"), true,
                    List.of(), metadata), c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            long[] v = new long[n];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell == null) {
                    continue;
                }
                if (!(cell instanceof PureDateLiteral d) || !d.precision().atLeast(PureDateLiteral.Precision.HOUR)) {
                    // a day under a TIMESTAMP column (a session hands one by design: JdbcDecode.temporal): the JSON
                    // route spells it as its day; a timestamp holds no day without a time of day invented, so here it
                    // is refused by name (the track A audit, finding 4; the decisions log D57)
                    throw refused(column, cell, "a timestamp (a date with a time of day; the JSON route spells"
                            + " such a cell as its day)");
                }
                int hour = 0, minute = 0, second = 0;
                long nanos = 0;
                switch (d) {
                    case PureDateLiteral.DateWithHour h -> hour = h.hour();
                    case PureDateLiteral.DateWithMinute m -> {
                        hour = m.hour();
                        minute = m.minute();
                    }
                    case PureDateLiteral.DateWithSecond s -> {
                        hour = s.hour();
                        minute = s.minute();
                        second = s.second();
                    }
                    case PureDateLiteral.DateWithSubsecond s -> {
                        hour = s.hour();
                        minute = s.minute();
                        second = s.second();
                        nanos = Long.parseLong(nanosDigits(column, s.subsecond()));
                    }
                    default -> throw refused(column, cell, "a timestamp (a date with a time of day)");
                }
                long seconds = java.time.LocalDateTime.of(year(d), month(d), day(d), hour, minute, second)
                        .toEpochSecond(java.time.ZoneOffset.UTC);
                try {
                    v[i] = Math.addExact(Math.multiplyExact(seconds, 1_000_000_000L), nanos);
                } catch (ArithmeticException outside) {
                    throw new IllegalStateException("column '" + column.name() + "': " + d
                            + " is outside the range a nanosecond timestamp holds (1678 to 2261)");
                }
            }
            out.buffer(ArrowIpc.int64s(v, n));
        }

        private static int year(PureDateLiteral d) {
            return switch (d) {
                case PureDateLiteral.DateWithHour h -> h.year();
                case PureDateLiteral.DateWithMinute m -> m.year();
                case PureDateLiteral.DateWithSecond s -> s.year();
                case PureDateLiteral.DateWithSubsecond s -> s.year();
                default -> throw new IllegalStateException("no time of day: " + d);
            };
        }

        private static int month(PureDateLiteral d) {
            return switch (d) {
                case PureDateLiteral.DateWithHour h -> h.month();
                case PureDateLiteral.DateWithMinute m -> m.month();
                case PureDateLiteral.DateWithSecond s -> s.month();
                case PureDateLiteral.DateWithSubsecond s -> s.month();
                default -> throw new IllegalStateException("no time of day: " + d);
            };
        }

        private static int day(PureDateLiteral d) {
            return switch (d) {
                case PureDateLiteral.DateWithHour h -> h.day();
                case PureDateLiteral.DateWithMinute m -> m.day();
                case PureDateLiteral.DateWithSecond s -> s.day();
                case PureDateLiteral.DateWithSubsecond s -> s.day();
                default -> throw new IllegalStateException("no time of day: " + d);
            };
        }
    }

    /** time64[ns]: a time of day as nanoseconds since midnight. */
    private static final class Time64 extends Enc {
        Time64(String name, Map<String, String> metadata, ExecutionPlan.Column c) {
            super(new ArrowField(name, new ArrowType.Time(ArrowType.TimeUnit.NANOSECOND, 64), true, List.of(),
                    metadata), c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            long[] v = new long[n];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell == null) {
                    continue;
                }
                v[i] = switch (cell) {
                    case PureTimeLiteral.TimeWithMinute t -> ofDay(t.hour(), t.minute(), 0, 0);
                    case PureTimeLiteral.TimeWithSecond t -> ofDay(t.hour(), t.minute(), t.second(), 0);
                    case PureTimeLiteral.TimeWithSubsecond t -> ofDay(t.hour(), t.minute(), t.second(),
                            Long.parseLong(nanosDigits(column, t.subsecond())));
                    default -> throw refused(column, cell, "a time64");
                };
            }
            out.buffer(ArrowIpc.int64s(v, n));
        }

        private static long ofDay(int hour, int minute, int second, long nanos) {
            return ((hour * 3600L) + (minute * 60L) + second) * 1_000_000_000L + nanos;
        }
    }

    /** list: 32-bit offsets into the element array, the elements laid out after. */
    private static final class ListOf extends Enc {
        private final Enc element;

        ListOf(String name, Map<String, String> metadata, ExecutionPlan.Column c, Enc element) {
            super(new ArrowField(name, new ArrowType.List(), true, List.of(element.field()), metadata), c);
            this.element = element;
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            int[] offsets = new int[n + 1];
            List<Object> elements = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell instanceof List<?> l) {
                    elements.addAll(l);
                } else if (cell != null) {
                    throw refused(column, cell, "a list");
                }
                offsets[i + 1] = elements.size();
            }
            out.buffer(ArrowIpc.int32s(offsets, n + 1));
            element.layout(elements.size(), elements::get, out);
        }
    }

    /** struct: one child array per field, each read out of the cell's map by the field's name. */
    private static final class StructOf extends Enc {
        private final List<Enc> fields;

        StructOf(String name, Map<String, String> metadata, ExecutionPlan.Column c, List<Enc> fields) {
            super(new ArrowField(name, new ArrowType.Struct(), true, fields.stream().map(Enc::field).toList(),
                    metadata), c);
            this.fields = fields;
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell != null && !(cell instanceof Map<?, ?>)) {
                    throw refused(column, cell, "a struct");
                }
            }
            for (Enc f : fields) {
                String key = f.field().name();
                f.layout(n, row -> {
                    Object cell = cells.apply(row);
                    return cell == null ? null : ((Map<?, ?>) cell).get(key);
                }, out);
            }
        }
    }

    /** map: offsets into the entries, a non-nullable struct of {@code key} (never null) and {@code value}. */
    private static final class MapOf extends Enc {
        private final Enc key;
        private final Enc value;

        MapOf(String name, Map<String, String> metadata, ExecutionPlan.Column c, Enc key, Enc value) {
            super(new ArrowField(name, new ArrowType.Map(false), true, List.of(new ArrowField("entries",
                    new ArrowType.Struct(), false, List.of(new ArrowField(key.field().name(), key.field().type(), false,
                            key.field().children(), key.field().metadata()), value.field()), Map.of())), metadata), c);
            this.key = key;
            this.value = value;
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            int[] offsets = new int[n + 1];
            List<Object> keys = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell instanceof Map<?, ?> m) {
                    for (Map.Entry<?, ?> e : m.entrySet()) {
                        if (e.getKey() == null) {
                            throw new IllegalStateException("column '" + column.name() + "': a map's key is null");
                        }
                        keys.add(e.getKey());
                        values.add(e.getValue());
                    }
                } else if (cell != null) {
                    throw refused(column, cell, "a map");
                }
                offsets[i + 1] = keys.size();
            }
            out.buffer(ArrowIpc.int32s(offsets, n + 1));
            // the entries struct: never null, so no validity bytes
            out.node(keys.size(), 0);
            out.buffer(new byte[0]);
            key.layout(keys.size(), keys::get, out);
            value.layout(values.size(), values::get, out);
        }
    }

    /** A tagged column ({@code TAGGED<kind slot, ...>}): a dense union of one member per slot, the kind the member's
     *  name. Each cell's member is the TAG the batch carries beside the carrier ({@link Batch#tag}: the decoders read
     *  the tag with the row and hand the batch a {@link TaggedCell}; track F, 2026-10-29 -- until then the member was
     *  inferred from the carrier's class, and two slots of one carrier were refused). A null is a null of the first
     *  member. A union nested in a structure has no tag in the batch: refused by name. */
    private static final class UnionOf extends Enc {
        private final List<Enc> members;

        UnionOf(String name, Map<String, String> metadata, ExecutionPlan.Column c, List<Enc> members) {
            // the type ids stated (0..n-1): spec-legal to omit them, but apache-arrow JS 17 (the browser's reader)
            // builds its member index from the vector and reads nothing without it (the track A audit, finding 1)
            super(new ArrowField(name, new ArrowType.Union(true,
                    java.util.stream.IntStream.range(0, members.size()).toArray()), true,
                    members.stream().map(Enc::field).toList(), metadata), c);
            this.members = members;
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            throw new IllegalStateException("column '" + column.name() + "' (slot " + column.slot()
                    + "): a union inside a structure: the batch carries a tag for a column's own cell alone");
        }

        void layout(int n, IntFunction<Object> cells, java.util.function.IntUnaryOperator tags, ArrowIpc.Layout out) {
            byte[] ids = new byte[n];
            int[] offsets = new int[n];
            List<List<Object>> held = new ArrayList<>();
            for (int k = 0; k < members.size(); k++) {
                held.add(new ArrayList<>());
            }
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                int member = cell == null ? 0 : tags.applyAsInt(i);
                if (member < 0 || member >= members.size()) {
                    throw new IllegalStateException("column '" + column.name() + "' (slot " + column.slot()
                            + "): the tag " + member + " names none of its " + members.size() + " slots");
                }
                ids[i] = (byte) member;
                offsets[i] = held.get(member).size();
                held.get(member).add(cell);
            }
            // a union has no validity bitmap: a null is a null of its member
            out.node(n, 0);
            out.buffer(ids);
            out.buffer(ArrowIpc.int32s(offsets, n));
            for (int k = 0; k < members.size(); k++) {
                List<Object> values = held.get(k);
                members.get(k).layout(values.size(), values::get, out);
            }
        }
    }
}
