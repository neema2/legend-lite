package com.legend.result;

import com.legend.arrowipc.ArrowField;
import com.legend.arrowipc.ArrowIpc;
import com.legend.arrowipc.ArrowType;
import com.legend.executionplan.ExecutionPlan;
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
 * LEGEND-ENGINE'S {@code ARROW_IPC} ANSWER, WRITTEN AS LEGEND-ENGINE WRITES IT (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md
 * R4 step 3; D17: an edge writer exactly as upstream's, for upstream's tools and users; D19: lite's own consumers read
 * lite's own Arrow, {@link ArrowWriter}). Upstream read in the pinned sources (legend-engine 4.145.0,
 * {@code RelationalResultToArrowIPCSerializer}, {@code RelationalResult.getValue}):
 * <ul>
 *   <li>each field typed from the DATABASE'S REPORT of its column alone ({@link ColumnReport}: the JDBC type code,
 *       its precision and scale; {@link #arrowType}, upstream's {@code jdbcToArrow}), named by the column's label,
 *       nullable, with no field metadata -- never from the plan's types;</li>
 *   <li>each value as upstream sets it: an integer narrowed to its width ({@code Number.byteValue} and kin), a float
 *       to 32 bits, a decimal at the reported scale, HALF_UP (D18 A: a misreported scale's loss copied at the edge),
 *       a date its day, a time its milliseconds, a timestamp its microseconds (finer digits dropped), every other
 *       column its value's text (a list its JSON as Jackson writes an array, a JSON column's text as Jackson writes
 *       the tree it reads);</li>
 *   <li>the schema's metadata in the order upstream's answer carries it ({@link #inHashMapOrder}: Java's HashMap,
 *       copied as Arrow copies it); record batches of {@value #BATCH} rows, none for an empty result. The caller
 *       frames the stream (one zstd frame).</li>
 * </ul>
 * Where upstream fails, lite answers (D18; register S133): a DuckDB {@code TIME} column, whose value upstream casts to
 * JDBC's {@code Time} and fails, is written as its milliseconds of the day. Where upstream fails on a list within a
 * list (Jackson cannot serialize the driver's nested array), lite refuses it. What this writer cannot yet write as
 * upstream does (a map's or a struct's text, a binary value's hex, a time with a zone; register S136) is refused by
 * name, never written some other way.
 */
public final class EngineArrowWriter implements ResultSink {

    /** Upstream's record batch size ({@code DEFAULT_BATCH_SIZE}). */
    public static final int BATCH = 1024;

    private final OutputStream out;
    private final @com.legend.base.Nullable Map<String, String> fixed;
    private final java.util.function.@com.legend.base.Nullable BiFunction<List<ExecutionPlan.Column>,
            @com.legend.base.Nullable List<@com.legend.base.Nullable String>, Map<String, String>> dynamic;
    private @com.legend.base.Nullable List<ColumnReport> report;
    private boolean reportGiven;
    private List<Enc> encoders = List.of();
    // the rows waiting for the next record batch: each one's batch and row there (a batch's cells read in place)
    private final Batch[] pendingBatch = new Batch[BATCH];
    private final int[] pendingRow = new int[BATCH];
    private int pending;

    /** Onto {@code out} with a fixed metadata (upstream's {@code legend.builder}, {@code legend.activities},
     *  {@code legend.generationInfo} where there is one, {@code legend.columns}), written in Java's HashMap order. */
    public EngineArrowWriter(OutputStream out, Map<String, String> schemaMetadata) {
        this.out = out;
        this.fixed = schemaMetadata;
        this.dynamic = null;
    }

    /** Onto {@code out}, a DYNAMIC result's (a pivot whose columns its data names): its metadata made from the columns
     *  the result announces and the database's report of each ({@link ResultSink#columns(List, List)}; D18 F). */
    public EngineArrowWriter(OutputStream out, java.util.function.BiFunction<List<ExecutionPlan.Column>,
            @com.legend.base.Nullable List<@com.legend.base.Nullable String>, Map<String, String>> schemaMetadata) {
        this.out = out;
        this.fixed = null;
        this.dynamic = schemaMetadata;
    }

    @Override
    public boolean wantsReport() {
        return true;
    }

    @Override
    public void reported(@com.legend.base.Nullable List<ColumnReport> reported) {
        this.report = reported;
        this.reportGiven = true;
    }

    @Override
    public void columns(List<ExecutionPlan.Column> columns, @com.legend.base.Nullable List<@com.legend.base.Nullable String> reported)
            throws IOException {
        if (dynamic == null) {
            columns(columns);
            return;
        }
        schema(columns, dynamic.apply(columns, reported));
    }

    @Override
    public void columns(List<ExecutionPlan.Column> columns) throws IOException {
        if (fixed == null) {
            throw new IllegalStateException("a dynamic result's metadata is made from the database's report: the run "
                    + "announced its columns without one");
        }
        schema(columns, fixed);
    }

    private void schema(List<ExecutionPlan.Column> columns, Map<String, String> metadata) throws IOException {
        List<ColumnReport> r = report;
        if (!reportGiven || r == null) {
            throw new IllegalStateException("legend-engine's Arrow answer is typed from the database's report of the"
                    + " result's columns (their JDBC types), and "
                    + (reportGiven ? "the database reports no column types" : "this run handed none"));
        }
        if (r.size() != columns.size()) {
            throw new IllegalStateException("the database reports " + r.size() + " columns for the " + columns.size()
                    + " the plan answers " + columns.stream().map(ExecutionPlan.Column::name).toList()
                    + ": a column of several slots (a mixed column) has no legend-engine Arrow type");
        }
        List<Enc> encs = new ArrayList<>(columns.size());
        List<ArrowField> fields = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            ColumnReport c = r.get(i);
            if (c == null) {
                throw new IllegalStateException("the database reports nothing of column '" + columns.get(i).name()
                        + "': legend-engine's Arrow answer is typed from its report");
            }
            ArrowType type = arrowType(c);
            fields.add(new ArrowField(c.label(), type, true, List.of(), Map.of()));
            encs.add(encoder(c, type));
        }
        encoders = encs;
        out.write(ArrowIpc.schema(fields, inHashMapOrder(metadata)));
    }

    @Override
    public void batch(Batch batch) throws IOException {
        for (int r = 0; r < batch.size(); r++) {
            pendingBatch[pending] = batch;
            pendingRow[pending] = r;
            pending++;
            if (pending == BATCH) {
                flush();
            }
        }
    }

    @Override
    public void end() throws IOException {
        if (pending > 0) {
            flush();
        }
        out.write(ArrowIpc.END);
        out.flush();
    }

    private void flush() throws IOException {
        int n = pending;
        ArrowIpc.Layout layout = new ArrowIpc.Layout();
        for (int i = 0; i < encoders.size(); i++) {
            int column = i;
            encoders.get(i).layout(n, row -> pendingBatch[row].get(pendingRow[row], column), layout);
        }
        out.write(ArrowIpc.recordBatch(n, layout, Map.of()));
        java.util.Arrays.fill(pendingBatch, 0, n, null);
        pending = 0;
    }

    // ---- upstream's rules ---------------------------------------------------------------------------------------

    /**
     * A column's Arrow type from the database's report, as upstream's {@code jdbcToArrow}: {@code BIT} and
     * {@code BOOLEAN} bool; {@code TINYINT} int8; {@code SMALLINT} int16; {@code INTEGER} int32; {@code BIGINT} int64;
     * {@code FLOAT} and {@code REAL} float32; {@code DOUBLE} float64; {@code NUMERIC} and {@code DECIMAL}
     * decimal128(p, s) (p 38 where reported 0 or less, at most 38; s at least 0, at most p); {@code DATE} date32;
     * {@code TIME} time32[ms]; {@code TIMESTAMP} and {@code TIMESTAMP_WITH_TIMEZONE} timestamp[us, UTC]; every other
     * code utf8 -- upstream's own default arm ("fallback (CHAR, VARCHAR, CLOB, etc.)"), copied, not a guess of lite's.
     */
    public static ArrowType arrowType(ColumnReport c) {
        return switch (c.jdbcType()) {
            case ColumnReport.BIT, ColumnReport.BOOLEAN -> new ArrowType.Bool();
            case ColumnReport.TINYINT -> new ArrowType.Int(8, true);
            case ColumnReport.SMALLINT -> new ArrowType.Int(16, true);
            case ColumnReport.INTEGER -> new ArrowType.Int(32, true);
            case ColumnReport.BIGINT -> new ArrowType.Int(64, true);
            case ColumnReport.FLOAT, ColumnReport.REAL -> new ArrowType.FloatingPoint(ArrowType.FloatPrecision.SINGLE);
            case ColumnReport.DOUBLE -> new ArrowType.FloatingPoint(ArrowType.FloatPrecision.DOUBLE);
            case ColumnReport.NUMERIC, ColumnReport.DECIMAL -> {
                int p = c.precision() <= 0 ? 38 : Math.min(c.precision(), 38);
                int s = Math.max(c.scale(), 0);
                yield new ArrowType.Decimal(p, s > p ? p : s, 128);
            }
            case ColumnReport.DATE -> new ArrowType.Date(ArrowType.DateUnit.DAY);
            case ColumnReport.TIME -> new ArrowType.Time(ArrowType.TimeUnit.MILLISECOND, 32);
            case ColumnReport.TIMESTAMP, ColumnReport.TIMESTAMP_WITH_TIMEZONE ->
                    new ArrowType.Timestamp(ArrowType.TimeUnit.MICROSECOND, "UTC");
            default -> new ArrowType.Utf8();
        };
    }

    /**
     * {@code metadata} in the order upstream's answer carries it on the wire: upstream builds it in a
     * {@code new HashMap<>()} (sixteen buckets), and Arrow's {@code Schema} and its stream writer each copy it with
     * {@code new HashMap<>(m)} ({@code Collections2.immutableMapCopy}: a table sized to the map,
     * {@code tableSizeFor(size / 0.75 + 1)} buckets), whose order the writer serializes. Each map iterates its buckets
     * in turn, a key's bucket its hash spread as {@code HashMap.hash} spreads it, a bucket's keys in the order they
     * came; a copy of a copy into a table of the same size keeps the order. Written out here rather than read from a
     * map of the host's, so every host's answer is the JVM's (a class library's own map need not iterate as
     * OpenJDK's does). Up to seven keys: no bucket can reach the eight that make a HashMap rebuild itself.
     */
    public static Map<String, String> inHashMapOrder(Map<String, String> metadata) {
        if (metadata.size() > 7) {
            throw new IllegalArgumentException("more than seven metadata keys can rebuild a HashMap: not written here");
        }
        List<Map.Entry<String, String>> upstream = bucketOrder(new ArrayList<>(metadata.entrySet()), 16);
        int copy = Integer.highestOneBit(Math.max(1, (int) (metadata.size() / 0.75f + 1.0f) - 1)) << 1;
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : bucketOrder(upstream, copy)) {
            out.put(e.getKey(), e.getValue());
        }
        return out;
    }

    /** {@code entries}, put in their order into a HashMap of {@code buckets} buckets, in its iteration order. */
    private static List<Map.Entry<String, String>> bucketOrder(List<Map.Entry<String, String>> entries, int buckets) {
        List<List<Map.Entry<String, String>>> table = new ArrayList<>(buckets);
        for (int b = 0; b < buckets; b++) {
            table.add(new ArrayList<>());
        }
        for (Map.Entry<String, String> e : entries) {
            int h = e.getKey().hashCode();
            table.get((h ^ (h >>> 16)) & (buckets - 1)).add(e);
        }
        List<Map.Entry<String, String>> out = new ArrayList<>(entries.size());
        for (List<Map.Entry<String, String>> bucket : table) {
            out.addAll(bucket);
        }
        return out;
    }

    private static Enc encoder(ColumnReport c, ArrowType type) {
        return switch (type) {
            case ArrowType.Bool b -> new Bools(c);
            case ArrowType.Int i -> new Ints(c, i.bits());
            case ArrowType.FloatingPoint f -> f.precision() == ArrowType.FloatPrecision.SINGLE ? new Float32(c)
                    : new Float64(c);
            case ArrowType.Decimal d -> new Decimal128(c, d.precision(), d.scale());
            case ArrowType.Date d -> new Date32(c);
            case ArrowType.Time t -> new Time32(c);
            case ArrowType.Timestamp t -> new TimestampMicros(c);
            case ArrowType.Utf8 u -> new Text(c);
            default -> throw new IllegalStateException("not an Arrow type upstream's jdbcToArrow answers: " + type);
        };
    }

    private static IllegalStateException refused(ColumnReport c, Object cell, String arrow) {
        return new IllegalStateException("column '" + c.label() + "' (" + c.driverType() + "): a cell "
                + cell.getClass().getSimpleName() + " " + cell + " is not what " + arrow + " holds");
    }

    // ---- the encoders: one per Arrow type upstream writes ------------------------------------------------------

    private abstract static sealed class Enc permits Bools, Ints, Float32, Float64, Decimal128, Date32, Time32,
            TimestampMicros, Text {
        final ColumnReport column;

        Enc(ColumnReport column) {
            this.column = column;
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

        Number number(Object cell, String arrow) {
            if (cell instanceof Number number) {
                return number;
            }
            throw refused(column, cell, arrow);
        }
    }

    private static final class Bools extends Enc {
        Bools(ColumnReport c) {
            super(c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            out.buffer(ArrowIpc.bits(n, i -> {
                Object cell = cells.apply(i);
                if (cell == null) {
                    return false;
                }
                if (cell instanceof Boolean b) {
                    return b;
                }
                throw refused(column, cell, "a bool");
            }));
        }
    }

    /** intN: the value narrowed to N bits as upstream narrows it ({@code Number.byteValue}, {@code shortValue},
     *  {@code intValue}, {@code longValue}). */
    private static final class Ints extends Enc {
        private final int bits;

        Ints(ColumnReport c, int bits) {
            super(c);
            this.bits = bits;
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            switch (bits) {
                case 8 -> {
                    byte[] v = new byte[n];
                    for (int i = 0; i < n; i++) {
                        Object cell = cells.apply(i);
                        v[i] = cell == null ? 0 : number(cell, "an int8").byteValue();
                    }
                    out.buffer(ArrowIpc.int8s(v, n));
                }
                case 16 -> {
                    short[] v = new short[n];
                    for (int i = 0; i < n; i++) {
                        Object cell = cells.apply(i);
                        v[i] = cell == null ? 0 : number(cell, "an int16").shortValue();
                    }
                    out.buffer(ArrowIpc.int16s(v, n));
                }
                case 32 -> {
                    int[] v = new int[n];
                    for (int i = 0; i < n; i++) {
                        Object cell = cells.apply(i);
                        v[i] = cell == null ? 0 : number(cell, "an int32").intValue();
                    }
                    out.buffer(ArrowIpc.int32s(v, n));
                }
                case 64 -> {
                    long[] v = new long[n];
                    for (int i = 0; i < n; i++) {
                        Object cell = cells.apply(i);
                        v[i] = cell == null ? 0 : number(cell, "an int64").longValue();
                    }
                    out.buffer(ArrowIpc.int64s(v, n));
                }
                default -> throw new IllegalStateException("no " + bits + "-bit integer is upstream's");
            }
        }
    }

    private static final class Float32 extends Enc {
        Float32(ColumnReport c) {
            super(c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            float[] v = new float[n];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                v[i] = cell == null ? 0 : number(cell, "a float32").floatValue();
            }
            out.buffer(ArrowIpc.float32s(v, n));
        }
    }

    private static final class Float64 extends Enc {
        Float64(ColumnReport c) {
            super(c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            double[] v = new double[n];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                v[i] = cell == null ? 0 : number(cell, "a float64").doubleValue();
            }
            out.buffer(ArrowIpc.float64s(v, n));
        }
    }

    /** decimal128(p, s): the value at the reported scale, HALF_UP, as upstream sets it ({@code setScale(s, HALF_UP)};
     *  a value of another kind read from its text, {@code new BigDecimal(value.toString())}); one with more digits
     *  than p refused by name, as upstream's vector refuses it. */
    private static final class Decimal128 extends Enc {
        private final int precision;
        private final int scale;

        Decimal128(ColumnReport c, int precision, int scale) {
            super(c);
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
                BigDecimal d = cell instanceof BigDecimal b ? b : new BigDecimal(number(cell, "a decimal128").toString());
                BigDecimal atScale = d.setScale(scale, java.math.RoundingMode.HALF_UP);
                if (atScale.precision() > precision) {
                    throw new IllegalStateException("column '" + column.label() + "': " + d.toPlainString()
                            + " has more digits than decimal128(" + precision + ", " + scale + ") holds");
                }
                v[i] = atScale.unscaledValue();
            }
            out.buffer(ArrowIpc.int128s(v, n));
        }
    }

    private static final class Date32 extends Enc {
        Date32(ColumnReport c) {
            super(c);
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

    /** time32[ms]: the time of day's milliseconds (upstream casts DuckDB's {@code LocalTime} to
     *  JDBC's {@code Time} and fails; lite answers, a register row). */
    private static final class Time32 extends Enc {
        Time32(ColumnReport c) {
            super(c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            int[] v = new int[n];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell == null) {
                    continue;
                }
                v[i] = switch (cell) {
                    case PureTimeLiteral.TimeWithMinute t -> millisOfDay(t.hour(), t.minute(), 0, "");
                    case PureTimeLiteral.TimeWithSecond t -> millisOfDay(t.hour(), t.minute(), t.second(), "");
                    case PureTimeLiteral.TimeWithSubsecond t -> millisOfDay(t.hour(), t.minute(), t.second(), t.subsecond());
                    default -> throw refused(column, cell, "a time32[ms]");
                };
            }
            out.buffer(ArrowIpc.int32s(v, n));
        }

        private static int millisOfDay(int hour, int minute, int second, String subsecond) {
            return ((hour * 3600) + (minute * 60) + second) * 1000 + leading(subsecond, 3);
        }
    }

    /** timestamp[us, UTC]: the instant's microseconds, finer digits dropped (upstream: {@code getTime() * 1000 +
     *  (getNanos() % 1_000_000) / 1000}); a day of no time of day its midnight (upstream reads every timestamp
     *  column with {@code getTimestamp}). */
    private static final class TimestampMicros extends Enc {
        TimestampMicros(ColumnReport c) {
            super(c);
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
                    case PureDateLiteral.StrictDate d -> micros(d.year(), d.month(), d.day(), 0, 0, 0, "");
                    case PureDateLiteral.DateWithHour h -> micros(h.year(), h.month(), h.day(), h.hour(), 0, 0, "");
                    case PureDateLiteral.DateWithMinute m ->
                            micros(m.year(), m.month(), m.day(), m.hour(), m.minute(), 0, "");
                    case PureDateLiteral.DateWithSecond s ->
                            micros(s.year(), s.month(), s.day(), s.hour(), s.minute(), s.second(), "");
                    case PureDateLiteral.DateWithSubsecond s ->
                            micros(s.year(), s.month(), s.day(), s.hour(), s.minute(), s.second(), s.subsecond());
                    default -> throw refused(column, cell, "a timestamp[us] (a date and a time of day)");
                };
            }
            out.buffer(ArrowIpc.int64s(v, n));
        }

        private long micros(int year, int month, int day, int hour, int minute, int second, String subsecond) {
            long seconds = java.time.LocalDateTime.of(year, month, day, hour, minute, second)
                    .toEpochSecond(java.time.ZoneOffset.UTC);
            try {
                return Math.addExact(Math.multiplyExact(seconds, 1_000_000L), leading(subsecond, 6));
            } catch (ArithmeticException outside) {
                throw new IllegalStateException("column '" + column.label() + "': an instant outside the range a"
                        + " microsecond timestamp holds");
            }
        }
    }

    /** The first {@code digits} digits of a written subsecond as a whole number (fewer stand for zeros after them;
     *  the rest dropped, as upstream drops a timestamp's digits past its unit). */
    private static int leading(String subsecond, int digits) {
        int value = 0;
        for (int i = 0; i < digits; i++) {
            int digit = i < subsecond.length() ? subsecond.charAt(i) - '0' : 0;
            if (digit < 0 || digit > 9) {
                throw new IllegalStateException("a subsecond of no digits: ." + subsecond);
            }
            value = value * 10 + digit;
        }
        return value;
    }

    /** utf8: the value's text, as upstream writes it ({@code value.toString()}): a text as it is, a whole number its
     *  digits (a sum of integers, DuckDB's {@code HUGEINT}), a list its JSON as Jackson writes an array's elements
     *  ({@code RelationalResult.getValue}). */
    private static final class Text extends Enc {
        Text(ColumnReport c) {
            super(c);
        }

        @Override
        void layout(int n, IntFunction<Object> cells, ArrowIpc.Layout out) {
            nullable(n, cells, out);
            byte[][] v = new byte[n][];
            for (int i = 0; i < n; i++) {
                Object cell = cells.apply(i);
                if (cell != null) {
                    v[i] = text(cell).getBytes(StandardCharsets.UTF_8);
                }
            }
            ArrowIpc.Varying varying = ArrowIpc.varying(v, n);
            out.buffer(varying.offsets());
            out.buffer(varying.data());
        }

        private String text(Object cell) {
            return switch (cell) {
                // a JSON column (DuckDB's JSON, a MAP, Snowflake's VARIANT: upstream's variant columns) re-spelled as
                // Jackson spells the tree it reads (RelationalResult.getValue: readTree(text).toString())
                case String s when variant(column) -> {
                    StringBuilder b = new StringBuilder();
                    jacksonTree(com.legend.json.Json.parse(s), b);
                    yield b.toString();
                }
                case String s -> s;
                case Long l -> l.toString();
                case Integer x -> x.toString();
                case BigInteger b -> b.toString();
                case BigDecimal d when d.scale() == 0 -> d.toString();
                case List<?> list when column.jdbcType() == ColumnReport.ARRAY -> {
                    StringBuilder b = new StringBuilder();
                    jackson(list, b);
                    yield b.toString();
                }
                default -> throw refused(column, cell, "upstream's text of a " + column.driverType() + " (to be read"
                        + " in upstream's sources before it is written)");
            };
        }

        /** Upstream's variant columns (SQLResultDBColumnsMetaData: a type name that begins JSON, MAP or VARIANT). */
        private static boolean variant(ColumnReport c) {
            String t = c.driverType().toUpperCase(java.util.Locale.ROOT);
            return t.startsWith("JSON") || t.startsWith("MAP") || t.startsWith("VARIANT");
        }

        /**
         * An array's elements as Jackson writes them (upstream: {@code writeValueAsString(array.getArray())}): a text
         * quoted and escaped, a whole number its digits, a double {@code Double.toString}'s, a decimal
         * {@code BigDecimal.toString}'s, a boolean, null. A list within a list is refused by name, as upstream fails on
         * it (Jackson cannot serialize the driver's nested array object).
         */
        private void jackson(Object value, StringBuilder b) {
            switch (value) {
                case List<?> list -> {
                    b.append('[');
                    for (int i = 0; i < list.size(); i++) {
                        if (i > 0) {
                            b.append(',');
                        }
                        Object e = list.get(i);
                        if (e == null) {
                            b.append("null");
                        } else if (e instanceof List<?>) {
                            throw refused(column, e, "upstream's JSON of an array's element (upstream fails on a list"
                                    + " within a list: Jackson cannot serialize the driver's nested array)");
                        } else {
                            jackson(e, b);
                        }
                    }
                    b.append(']');
                }
                case String s -> quoted(s, b);
                case Long x -> b.append(x);
                case Integer x -> b.append(x);
                case BigInteger x -> b.append(x);
                case Boolean x -> b.append(x);
                case Double x when Double.isFinite(x) -> b.append(x);
                case BigDecimal x -> b.append(x);
                default -> throw refused(column, value, "upstream's JSON of an array's element (only a text, a number"
                        + " and a boolean are read in upstream's sources so far)");
            }
        }

        /** A JSON document as Jackson writes the tree it reads ({@code readTree(text).toString()}): compact, an
         *  object's keys in their order, a whole number its digits, a number with a fraction or an exponent the
         *  double it reads ({@code Double.toString}), a text escaped as Jackson escapes it. */
        private void jacksonTree(com.legend.json.Json.Node node, StringBuilder b) {
            switch (node) {
                case com.legend.json.Json.Obj o -> {
                    b.append('{');
                    boolean first = true;
                    for (Map.Entry<String, com.legend.json.Json.Node> e : o.fields().entrySet()) {
                        if (!first) {
                            b.append(',');
                        }
                        first = false;
                        quoted(e.getKey(), b);
                        b.append(':');
                        jacksonTree(e.getValue(), b);
                    }
                    b.append('}');
                }
                case com.legend.json.Json.Arr a -> {
                    b.append('[');
                    for (int i = 0; i < a.items().size(); i++) {
                        if (i > 0) {
                            b.append(',');
                        }
                        jacksonTree(a.items().get(i), b);
                    }
                    b.append(']');
                }
                case com.legend.json.Json.Str x -> quoted(x.value(), b);
                case com.legend.json.Json.Bool x -> b.append(x.value());
                case com.legend.json.Json.Null x -> b.append("null");
                case com.legend.json.Json.Num x -> {
                    String token = x.token();
                    if (token == null && x.isInteger()) {
                        // a whole number within a long (the parser keeps no text for one): Jackson's int or long node,
                        // its digits ("-0" is the int 0)
                        b.append(x.longValue());
                    } else if (token == null) {
                        throw refused(column, x, "a JSON number read without its text");
                    } else if (token.indexOf('.') < 0 && token.indexOf('e') < 0 && token.indexOf('E') < 0) {
                        // a whole number past a long: Jackson's BigInteger node, its digits
                        b.append(new BigInteger(token));
                    } else {
                        // a fraction or an exponent: the double Jackson reads, as it writes one
                        b.append(Double.toString(Double.parseDouble(token)));
                    }
                }
            }
        }

        private static final String HEX = "0123456789ABCDEF";

        /** Jackson's default string escaping: the quote and the backslash, the five short escapes, every other
         *  control character as {@code \\u00XX}; nothing else escaped. */
        private static void quoted(String s, StringBuilder b) {
            b.append('"');
            for (int i = 0; i < s.length(); i++) {
                char ch = s.charAt(i);
                switch (ch) {
                    case '"' -> b.append("\\\"");
                    case '\\' -> b.append("\\\\");
                    case '\b' -> b.append("\\b");
                    case '\f' -> b.append("\\f");
                    case '\n' -> b.append("\\n");
                    case '\r' -> b.append("\\r");
                    case '\t' -> b.append("\\t");
                    default -> {
                        if (ch < 0x20) {
                            // upper-case hex digits, as Jackson writes them (CharTypes.HC)
                            b.append("\\u00").append(HEX.charAt(ch >> 4)).append(HEX.charAt(ch & 15));
                        } else {
                            b.append(ch);
                        }
                    }
                }
            }
            b.append('"');
        }
    }
}
