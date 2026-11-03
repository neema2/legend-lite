// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.arrowipc.ArrowField;
import com.legend.arrowipc.ArrowIpc;
import com.legend.arrowipc.ArrowType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE CORE'S ARROW READER (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R3): a host's rows handed over as the Arrow its
 * database answered decode to the cells the same rows decode to as JSON text ({@link HostRows}, the handoff the hosts
 * use today) and as a JDBC session reads them -- so the writers' answers are the same bytes. Its streams are built with
 * the repository's own framer ({@link ArrowIpc}) in the encodings DuckDB answers (an INTEGER an Int32, a HUGEINT a
 * 128-bit decimal of scale 0, a TIMESTAMP microseconds with no zone, a TIMESTAMPTZ with one, a TIME a Time64 of
 * microseconds), and in lite's own Arrow writer's (a round trip). The real databases' Arrow is judged in the hosts'
 * own suites (//python:bindings_test, //engine-client:plan_runner_test).
 */
class ArrowRowsTest {

    private static ExecutionPlan.Column col(String name, String type, String slot) {
        boolean many = slot != null && slot.startsWith("ARRAY<");
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(0, many ? null : 1), slot, null,
                null, null);
    }

    private static List<List<Object>> decoded(byte[] arrow, List<ExecutionPlan.Column> columns) throws IOException {
        Collector c = new Collector();
        ArrowRows.stream(ByteBuffer.wrap(arrow), columns, c);
        return c.collected().rows();
    }

    private static List<List<Object>> decoded(String rowsJson, List<ExecutionPlan.Column> columns) throws IOException {
        Collector c = new Collector();
        HostRows.stream(rowsJson, columns, c);
        return c.collected().rows();
    }

    /** {@code rows} written by legend-engine's writer (the answer a host's execute gives) and as lite's JSON rows: the
     *  writers spell the carriers, so equal texts are equal answers. */
    private static String written(List<ExecutionPlan.Column> columns, List<List<Object>> rows) throws IOException {
        StringWriter engine = new StringWriter();
        StringWriter lite = new StringWriter();
        List<String> names = columns.stream().map(ExecutionPlan.Column::name).toList();
        List<ResultSink> writers = List.of(
                PureV1Writer.relation(engine, announced -> new PureV1Writer.Envelope("{}", "[]", names)),
                new JsonRowsWriter(lite, ExecutionPlan.TdsResult.Shape.RELATION));
        for (ResultSink w : writers) {
            w.columns(columns);
            if (!rows.isEmpty()) {
                Batch.Builder b = Batch.builder(columns, rows.size());
                rows.forEach(b::add);
                w.batch(b.build());
            }
            w.end();
        }
        return engine + "\n" + lite;
    }

    private static List<Object> row(Object... cells) {
        return Arrays.asList(cells);
    }

    // -- a stream in DuckDB's encodings, built with the framer -------------------------------

    /** One column of a hand-built batch: its field and how it lays itself out. */
    private record Col(ArrowField field, java.util.function.Consumer<ArrowIpc.Layout> layout) {
    }

    private static byte[] stream(int rows, List<Col> cols) {
        List<ArrowField> fields = cols.stream().map(Col::field).toList();
        ArrowIpc.Layout layout = new ArrowIpc.Layout();
        cols.forEach(c -> c.layout().accept(layout));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ArrowIpc.schema(fields, Map.of()));
        out.writeBytes(ArrowIpc.recordBatch(rows, layout, Map.of()));
        out.writeBytes(ArrowIpc.END);
        return out.toByteArray();
    }

    private static byte[] le(int width, long... values) {
        ByteBuffer b = ByteBuffer.allocate(width * values.length).order(ByteOrder.LITTLE_ENDIAN);
        for (long v : values) {
            switch (width) {
                case 1 -> b.put((byte) v);
                case 2 -> b.putShort((short) v);
                case 4 -> b.putInt((int) v);
                default -> b.putLong(v);
            }
        }
        return b.array();
    }

    /** A fixed-width column of three values, the second null. */
    private static Col fixed(String name, ArrowType type, byte[] values) {
        return new Col(new ArrowField(name, type, true), l -> {
            l.node(3, 1);
            l.buffer(ArrowIpc.validity(3, i -> i == 1));
            l.buffer(values);
        });
    }

    private static Col text(String name, ArrowType type, String a, String c) {
        byte[][] values = {a.getBytes(StandardCharsets.UTF_8), null, c.getBytes(StandardCharsets.UTF_8)};
        ArrowIpc.Varying v = ArrowIpc.varying(values, 3);
        boolean large = type instanceof ArrowType.LargeUtf8;
        byte[] offsets = v.offsets();
        if (large) {
            ByteBuffer in = ByteBuffer.wrap(offsets).order(ByteOrder.LITTLE_ENDIAN);
            long[] wide = new long[4];
            for (int i = 0; i < 4; i++) {
                wide[i] = in.getInt(4 * i);
            }
            offsets = ArrowIpc.int64s(wide, 4);
        }
        byte[] o = offsets;
        return new Col(new ArrowField(name, type, true), l -> {
            l.node(3, 1);
            l.buffer(ArrowIpc.validity(3, i -> i == 1));
            l.buffer(o);
            l.buffer(v.data());
        });
    }

    private static final long MICROS_2024_02_29_13_14_15_123456 =
            java.time.LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_456_000).toEpochSecond(java.time.ZoneOffset.UTC)
                    * 1_000_000L + 123_456L;

    @Test
    void duckDbsArrowDecodesAsTheSameRowsAsJsonText() throws IOException {
        // every scalar kind as DuckDB answers it in Arrow, and the same rows as a host's driver hands them as JSON text
        // (Python's compiler.host_rows: an integer's digits, a float's shortest digits, a decimal, a date, a time and a
        // date-time as their text): the cells are equal, so the writers' answers are the same bytes
        List<ExecutionPlan.Column> columns = List.of(
                col("i32", "Integer", "INTEGER"), col("i16", "Integer", "SMALLINT"), col("i8", "Integer", "TINYINT"),
                col("i64", "Integer", "BIGINT"), col("u64", "Integer", "UBIGINT"),
                col("huge", "Integer", "HUGEINT"),
                col("f32", "Float", "FLOAT"), col("f64", "Float", "DOUBLE"),
                col("dec", "Decimal", "DECIMAL(18,2)"),
                col("s", "String", "VARCHAR"), col("ls", "String", "VARCHAR"),
                col("b", "Boolean", "BOOLEAN"),
                col("day", "StrictDate", "DATE"), col("t", "StrictTime", "TIME"),
                col("ts", "DateTime", "TIMESTAMP"), col("tz", "DateTime", "TIMESTAMPTZ"),
                col("nothing", "Integer", "INTEGER"));
        BigInteger huge = new BigInteger("170141183460469231731687303715884105727");
        byte[] arrow = stream(3, List.of(
                fixed("i32", new ArrowType.Int(32, true), le(4, -2147483648L, 0, 7)),
                fixed("i16", new ArrowType.Int(16, true), le(2, -32768, 0, 32767)),
                fixed("i8", new ArrowType.Int(8, true), le(1, -128, 0, 127)),
                fixed("i64", new ArrowType.Int(64, true), le(8, Long.MIN_VALUE, 0, Long.MAX_VALUE)),
                fixed("u64", new ArrowType.Int(64, false), le(8, -1L, 0, 5)),
                fixed("huge", new ArrowType.Decimal(38, 0, 128), ArrowIpc.int128s(
                        new BigInteger[] {huge.negate(), null, BigInteger.valueOf(42)}, 3)),
                fixed("f32", new ArrowType.FloatingPoint(ArrowType.FloatPrecision.SINGLE),
                        ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putFloat(0.1f).putFloat(0).putFloat(-2.5f)
                                .array()),
                fixed("f64", new ArrowType.FloatingPoint(ArrowType.FloatPrecision.DOUBLE),
                        ArrowIpc.float64s(new double[] {0.1, 0, -0.0}, 3)),
                fixed("dec", new ArrowType.Decimal(18, 2, 128), ArrowIpc.int128s(
                        new BigInteger[] {BigInteger.valueOf(-1), null, BigInteger.valueOf(123456)}, 3)),
                text("s", new ArrowType.Utf8(), "héllo, \"quoted\" 漢字🙂", ""),
                text("ls", new ArrowType.LargeUtf8(), "large", "x"),
                new Col(new ArrowField("b", new ArrowType.Bool(), true), l -> {
                    l.node(3, 1);
                    l.buffer(ArrowIpc.validity(3, i -> i == 1));
                    l.buffer(ArrowIpc.bits(3, i -> i == 0));
                }),
                fixed("day", new ArrowType.Date(ArrowType.DateUnit.DAY), le(4, 19782, 0, -1)),
                fixed("t", new ArrowType.Time(ArrowType.TimeUnit.MICROSECOND, 64),
                        le(8, (13 * 3600 + 14 * 60 + 15) * 1_000_000L + 5, 0, 0)),
                fixed("ts", new ArrowType.Timestamp(ArrowType.TimeUnit.MICROSECOND, null),
                        le(8, MICROS_2024_02_29_13_14_15_123456, 0, -1)),
                fixed("tz", new ArrowType.Timestamp(ArrowType.TimeUnit.MICROSECOND, "UTC"),
                        le(8, MICROS_2024_02_29_13_14_15_123456, 0, 0)),
                new Col(new ArrowField("nothing", new ArrowType.Null(), true), l -> l.node(3, 3))));
        String json = "[{\"i32\":-2147483648,\"i16\":-32768,\"i8\":-128,\"i64\":-9223372036854775808,"
                + "\"u64\":18446744073709551615,\"huge\":-" + huge + ",\"f32\":0.1,\"f64\":0.1,\"dec\":\"-0.01\","
                + "\"s\":\"héllo, \\\"quoted\\\" 漢字🙂\",\"ls\":\"large\",\"b\":true,\"day\":\"2024-02-29\","
                + "\"t\":\"13:14:15.000005\",\"ts\":\"2024-02-29T13:14:15.123456\","
                + "\"tz\":\"2024-02-29T13:14:15.123456\",\"nothing\":null},"
                + "{\"i32\":null,\"i16\":null,\"i8\":null,\"i64\":null,\"u64\":null,\"huge\":null,\"f32\":null,"
                + "\"f64\":null,\"dec\":null,\"s\":null,\"ls\":null,\"b\":null,\"day\":null,\"t\":null,\"ts\":null,"
                + "\"tz\":null,\"nothing\":null},"
                + "{\"i32\":7,\"i16\":32767,\"i8\":127,\"i64\":9223372036854775807,\"u64\":5,\"huge\":42,"
                + "\"f32\":-2.5,\"f64\":-0.0,\"dec\":\"1234.56\",\"s\":\"\",\"ls\":\"x\",\"b\":false,"
                + "\"day\":\"1969-12-31\",\"t\":\"00:00:00\",\"ts\":\"1969-12-31T23:59:59.999999\","
                + "\"tz\":\"1970-01-01T00:00:00\",\"nothing\":null}]";
        List<List<Object>> fromArrow = decoded(arrow, columns);
        assertEquals(written(columns, decoded(json, columns)), written(columns, fromArrow));
        // the carriers, by name: a HUGEINT past a long an exact BigInteger, a UBIGINT past 63 bits too, a REAL its own
        // four-byte digits, a decimal at its scale, a timestamp with nine subsecond digits (the database's read)
        assertEquals(huge.negate(), fromArrow.get(0).get(5));
        assertEquals(new BigInteger("18446744073709551615"), fromArrow.get(0).get(4));
        assertEquals(0.1, fromArrow.get(0).get(6));
        assertEquals(new BigDecimal("-0.01"), fromArrow.get(0).get(8));
        assertEquals(PureDateLiteral.parse("2024-02-29T13:14:15.123456000"), fromArrow.get(0).get(14));
        assertEquals(PureTimeLiteral.parse("13:14:15.000005"), fromArrow.get(0).get(13));
        assertEquals(Arrays.asList(new Object[columns.size()]), fromArrow.get(1));
    }

    @Test
    void structuresDecodeByTheirSlots() throws IOException {
        // a list of DuckDB's INTEGERs, a struct and a map, read by the slot's declared layout, as JSON text reads them
        List<ExecutionPlan.Column> columns = List.of(
                col("l", "Integer", "ARRAY<INTEGER>"),
                col("st", "Any", "STRUCT<x BIGINT, y VARCHAR>"),
                col("m", "Any", "MAP<VARCHAR,BIGINT>"));
        ArrowField element = new ArrowField("item", new ArrowType.Int(32, true), true);
        ArrowField x = new ArrowField("x", new ArrowType.Int(64, true), true);
        ArrowField y = new ArrowField("y", new ArrowType.Utf8(), true);
        ArrowField key = new ArrowField("key", new ArrowType.Utf8(), false);
        ArrowField value = new ArrowField("value", new ArrowType.Int(64, true), true);
        ArrowField entries = new ArrowField("entries", new ArrowType.Struct(), false, List.of(key, value), Map.of());
        byte[] arrow = stream(2, List.of(
                new Col(new ArrowField("l", new ArrowType.List(), true, List.of(element), Map.of()), l -> {
                    l.node(2, 0);
                    l.buffer(new byte[0]);
                    l.buffer(le(4, 0, 3, 3));
                    l.node(3, 1);
                    l.buffer(ArrowIpc.validity(3, i -> i == 1));
                    l.buffer(le(4, 1, 0, -3));
                }),
                new Col(new ArrowField("st", new ArrowType.Struct(), true, List.of(x, y), Map.of()), l -> {
                    l.node(2, 1);
                    l.buffer(ArrowIpc.validity(2, i -> i == 1));
                    l.node(2, 0);
                    l.buffer(new byte[0]);
                    l.buffer(le(8, 7, 0));
                    ArrowIpc.Varying v = ArrowIpc.varying(new byte[][] {null, null}, 2);
                    l.node(2, 2);
                    l.buffer(ArrowIpc.validity(2, i -> true));
                    l.buffer(v.offsets());
                    l.buffer(v.data());
                }),
                new Col(new ArrowField("m", new ArrowType.Map(false), true, List.of(entries), Map.of()), l -> {
                    l.node(2, 0);
                    l.buffer(new byte[0]);
                    l.buffer(le(4, 0, 2, 2));
                    l.node(2, 0);
                    l.buffer(new byte[0]);
                    ArrowIpc.Varying k = ArrowIpc.varying(new byte[][] {"k".getBytes(StandardCharsets.UTF_8),
                            "j".getBytes(StandardCharsets.UTF_8)}, 2);
                    l.node(2, 0);
                    l.buffer(new byte[0]);
                    l.buffer(k.offsets());
                    l.buffer(k.data());
                    l.node(2, 1);
                    l.buffer(ArrowIpc.validity(2, i -> i == 0));
                    l.buffer(le(8, 0, 2));
                })));
        String json = "[{\"l\":[1,null,-3],\"st\":{\"x\":7,\"y\":null},\"m\":{\"k\":null,\"j\":2}},"
                + "{\"l\":[],\"st\":null,\"m\":{}}]";
        List<List<Object>> fromArrow = decoded(arrow, columns);
        assertEquals(decoded(json, columns), fromArrow);
        assertEquals(written(columns, decoded(json, columns)), written(columns, fromArrow));
        Map<String, Object> st = new LinkedHashMap<>();
        st.put("x", 7L);
        st.put("y", null);
        assertEquals(row(Arrays.asList(1L, null, -3L), st, fromArrow.get(0).get(2)), fromArrow.get(0));
    }

    @Test
    void liteArrowRoundTripsThroughTheReader() throws IOException {
        // lite's own Arrow writer's stream read back: the same cells, so the same answers (the writer's clean types:
        // an Int64, a decimal at the column's scale, a timestamp of nanoseconds at UTC, a Time64 of nanoseconds)
        List<ExecutionPlan.Column> columns = List.of(
                col("i", "Integer", "BIGINT"), col("h", "Integer", "HUGEINT"), col("f", "Float", "DOUBLE"),
                col("d", "Decimal", "DECIMAL(10,2)"), col("b", "Boolean", "BOOLEAN"), col("s", "String", "VARCHAR"),
                col("day", "StrictDate", "DATE"), col("at", "DateTime", "TIMESTAMP"), col("t", "StrictTime", "TIME"),
                col("l", "Integer", "ARRAY<BIGINT>"));
        List<List<Object>> rows = new ArrayList<>(List.of(
                row(1L, new BigInteger("99999999999999999999999999999999999999"), 1.5, new BigDecimal("12.30"), true,
                        "héllo", PureDateLiteral.parse("2024-02-29"),
                        PureDateLiteral.parse("2024-02-29T13:14:15.123456789"), PureTimeLiteral.parse("13:14:15.5"),
                        List.of(1L, 2L)),
                row(-9223372036854775808L, BigInteger.valueOf(-5), -0.0, new BigDecimal("-0.01"), false, "",
                        PureDateLiteral.parse("1969-12-31"), PureDateLiteral.parse("1969-12-31T23:59:59"),
                        PureTimeLiteral.parse("00:00:00"), List.of())));
        rows.add(Arrays.asList(new Object[columns.size()]));
        ByteArrayOutputStream arrow = new ByteArrayOutputStream();
        ArrowWriter w = new ArrowWriter(arrow, Map.of());
        w.columns(columns);
        Batch.Builder b = Batch.builder(columns, rows.size());
        rows.forEach(b::add);
        w.batch(b.build());
        w.end();
        assertEquals(written(columns, rows), written(columns, decoded(arrow.toByteArray(), columns)));
    }

    @Test
    void aTaggedColumnReadsItsTagAndItsSlotBesideIt() throws IOException {
        // a mixed value's column as a database answers it: the tag (the slot index) in the column's place, each slot's
        // value in a column of its own beside it, found by name (as the JDBC decode finds it)
        ExecutionPlan.Column v = col("v", "Any", "TAGGED<Integer BIGINT, String VARCHAR>");
        List<ExecutionPlan.Column> columns = List.of(v);
        List<ExecutionPlan.Column> slots = RowDecode.taggedSlots(v);
        byte[][] texts = {null, null, "a".getBytes(StandardCharsets.UTF_8)};
        ArrowIpc.Varying s = ArrowIpc.varying(texts, 3);
        byte[] arrow = stream(3, List.of(
                fixed("v", new ArrowType.Int(32, true), le(4, 0, 0, 1)),
                new Col(new ArrowField(slots.get(0).name(), new ArrowType.Int(64, true), true), l -> {
                    l.node(3, 2);
                    l.buffer(ArrowIpc.validity(3, i -> i != 0));
                    l.buffer(le(8, 9, 0, 0));
                }),
                new Col(new ArrowField(slots.get(1).name(), new ArrowType.Utf8(), true), l -> {
                    l.node(3, 2);
                    l.buffer(ArrowIpc.validity(3, i -> i != 2));
                    l.buffer(s.offsets());
                    l.buffer(s.data());
                })));
        // the second row's tag is null (fixed(...) nulls the second value): the cell is null; a batch hands each cell's
        // carrier and keeps its tag beside it
        List<Object> carriers = new ArrayList<>();
        List<Integer> tags = new ArrayList<>();
        ArrowRows.stream(ByteBuffer.wrap(arrow), columns, new ResultSink() {
            @Override
            public void columns(List<ExecutionPlan.Column> announced) {
            }

            @Override
            public void batch(Batch batch) {
                for (int r = 0; r < batch.size(); r++) {
                    carriers.add(batch.get(r, 0));
                    tags.add(batch.tag(r, 0));
                }
            }

            @Override
            public void end() {
            }
        });
        assertEquals(Arrays.asList(9L, null, "a"), carriers);
        assertEquals(List.of(0, -1, 1), tags);
    }

    @Test
    void anArrowFileIsReadAsTheStreamItHolds() throws IOException {
        // the file format (DuckDB-WASM's runQuery answers one): the magic and its padding, the stream's messages, a
        // footer indexing them, its length and the magic -- the same rows as the stream alone
        List<ExecutionPlan.Column> columns = List.of(col("i", "Integer", "INTEGER"));
        byte[] stream = stream(3, List.of(fixed("i", new ArrowType.Int(32, true), le(4, 1, 0, 3))));
        byte[] footer = {1, 2, 3, 4, 5, 6, 7, 8};
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.writeBytes("ARROW1\0\0".getBytes(StandardCharsets.US_ASCII));
        file.writeBytes(stream);
        file.writeBytes(footer);
        file.writeBytes(le(4, footer.length));
        file.writeBytes("ARROW1".getBytes(StandardCharsets.US_ASCII));
        assertEquals(decoded(stream, columns), decoded(file.toByteArray(), columns));
        assertEquals(Arrays.asList(1L, null, 3L), decoded(file.toByteArray(), columns).stream().map(r -> r.get(0)).toList());
    }

    @Test
    void whatTheReaderDoesNotReadIsRefusedByName() {
        List<ExecutionPlan.Column> one = List.of(col("d", "DateTime", "TIMESTAMP"));
        // a stream of another width than the plan's statement
        byte[] two = stream(3, List.of(fixed("a", new ArrowType.Int(32, true), le(4, 1, 2, 3)),
                fixed("b", new ArrowType.Int(32, true), le(4, 1, 2, 3))));
        IllegalArgumentException width = assertThrows(IllegalArgumentException.class, () -> decoded(two, one));
        assertTrue(width.getMessage().contains("answers 2 columns"), width.getMessage());
        // a value the column's kind does not read: a boolean under a TIMESTAMP slot
        byte[] bool = stream(3, List.of(new Col(new ArrowField("d", new ArrowType.Bool(), true), l -> {
            l.node(3, 0);
            l.buffer(new byte[0]);
            l.buffer(ArrowIpc.bits(3, i -> true));
        })));
        IllegalStateException kind = assertThrows(IllegalStateException.class, () -> decoded(bool, one));
        assertTrue(kind.getMessage().contains("is not of that kind"), kind.getMessage());
        // a type whose layout is not read: an interval
        byte[] interval = stream(3, List.of(fixed("d", new ArrowType.Interval(ArrowType.IntervalUnit.YEAR_MONTH),
                le(4, 1, 2, 3))));
        IllegalArgumentException unread = assertThrows(IllegalArgumentException.class, () -> decoded(interval, one));
        assertTrue(unread.getMessage().contains("not read"), unread.getMessage());
        // not a stream at all
        IllegalArgumentException empty = assertThrows(IllegalArgumentException.class,
                () -> decoded(new byte[0], one));
        assertTrue(empty.getMessage().contains("no schema"), empty.getMessage());
    }
}
