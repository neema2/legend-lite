package com.legend.result;

import com.legend.arrowipc.ArrowField;
import com.legend.arrowipc.ArrowReader;
import com.legend.arrowipc.ArrowType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.values.PureDateLiteral;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * legend-engine's ARROW_IPC answer as upstream writes it (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R4 step 3): each
 * field typed from the database's report alone, by upstream's {@code jdbcToArrow}; the schema's metadata in Java
 * HashMap's order; each value as upstream's {@code setValue} sets it; record batches of 1,024 rows. The servers' own
 * answers are the conformance suite's (//datacube:conformance_test, its arrow checks).
 */
class EngineArrowWriterTest {

    private static ExecutionPlan.Column col(String name, String type) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(0, 1), null, null, null, null);
    }

    private static ColumnReport report(String label, int code, int precision, int scale) {
        return new ColumnReport(label, "T", code, precision, scale);
    }

    @Test
    void eachJdbcTypeIsUpstreamsArrowType() {
        assertEquals(new ArrowType.Bool(), EngineArrowWriter.arrowType(report("b", ColumnReport.BIT, 1, 0)));
        assertEquals(new ArrowType.Bool(), EngineArrowWriter.arrowType(report("b", ColumnReport.BOOLEAN, 1, 0)));
        assertEquals(new ArrowType.Int(8, true), EngineArrowWriter.arrowType(report("i", ColumnReport.TINYINT, 3, 0)));
        assertEquals(new ArrowType.Int(16, true), EngineArrowWriter.arrowType(report("i", ColumnReport.SMALLINT, 5, 0)));
        assertEquals(new ArrowType.Int(32, true), EngineArrowWriter.arrowType(report("i", ColumnReport.INTEGER, 10, 0)));
        assertEquals(new ArrowType.Int(64, true), EngineArrowWriter.arrowType(report("i", ColumnReport.BIGINT, 19, 0)));
        assertEquals(new ArrowType.FloatingPoint(ArrowType.FloatPrecision.SINGLE),
                EngineArrowWriter.arrowType(report("f", ColumnReport.FLOAT, 0, 0)));
        assertEquals(new ArrowType.FloatingPoint(ArrowType.FloatPrecision.SINGLE),
                EngineArrowWriter.arrowType(report("f", ColumnReport.REAL, 0, 0)));
        assertEquals(new ArrowType.FloatingPoint(ArrowType.FloatPrecision.DOUBLE),
                EngineArrowWriter.arrowType(report("f", ColumnReport.DOUBLE, 0, 0)));
        assertEquals(new ArrowType.Decimal(18, 2, 128), EngineArrowWriter.arrowType(report("d", ColumnReport.DECIMAL, 18, 2)));
        // p 38 where reported 0 or less, at most 38; s at least 0, at most p
        assertEquals(new ArrowType.Decimal(38, 0, 128), EngineArrowWriter.arrowType(report("d", ColumnReport.NUMERIC, 0, -3)));
        assertEquals(new ArrowType.Decimal(38, 10, 128), EngineArrowWriter.arrowType(report("d", ColumnReport.DECIMAL, 65, 10)));
        assertEquals(new ArrowType.Decimal(5, 5, 128), EngineArrowWriter.arrowType(report("d", ColumnReport.DECIMAL, 5, 9)));
        assertEquals(new ArrowType.Date(ArrowType.DateUnit.DAY), EngineArrowWriter.arrowType(report("d", ColumnReport.DATE, 0, 0)));
        assertEquals(new ArrowType.Time(ArrowType.TimeUnit.MILLISECOND, 32),
                EngineArrowWriter.arrowType(report("t", ColumnReport.TIME, 0, 0)));
        for (int code : new int[] {ColumnReport.TIMESTAMP, ColumnReport.TIMESTAMP_WITH_TIMEZONE}) {
            assertEquals(new ArrowType.Timestamp(ArrowType.TimeUnit.MICROSECOND, "UTC"),
                    EngineArrowWriter.arrowType(report("t", code, 0, 0)));
        }
        // upstream's default arm: utf8 (a VARCHAR, DuckDB's HUGEINT and UUID as OTHER, a list, a struct)
        for (int code : new int[] {ColumnReport.VARCHAR, ColumnReport.OTHER, ColumnReport.ARRAY, ColumnReport.STRUCT,
                ColumnReport.TIME_WITH_TIMEZONE}) {
            assertEquals(new ArrowType.Utf8(), EngineArrowWriter.arrowType(report("s", code, 0, 0)));
        }
    }

    @Test
    void theMetadataIsInTheOrderUpstreamsArrowWritesIt() {
        // upstream builds a new HashMap<>(); Arrow's Schema and its stream writer each copy it (new HashMap<>(m)),
        // and the stream carries the copy's order
        List<List<String>> keySets = List.of(
                List.of("legend.builder", "legend.activities", "legend.columns"),
                List.of("legend.builder", "legend.activities", "legend.generationInfo", "legend.columns"),
                List.of("a", "b", "q", "legend.x", "zz", "legend.columns", "x1"),
                List.of("x17", "legend.columns"));
        for (List<String> keys : keySets) {
            Map<String, String> inserted = new LinkedHashMap<>();
            Map<String, String> upstream = new HashMap<>();
            for (String k : keys) {
                inserted.put(k, "v" + k);
                upstream.put(k, "v" + k);
            }
            Map<String, String> wire = new HashMap<>(new HashMap<>(upstream));
            assertEquals(List.copyOf(wire.keySet()), List.copyOf(EngineArrowWriter.inHashMapOrder(inserted).keySet()),
                    keys.toString());
        }
    }

    @Test
    void aTextColumnIsWrittenAsUpstreamWritesIt() throws IOException {
        List<ExecutionPlan.Column> columns = List.of(col("l", "String"), col("j", "Variant"));
        List<ColumnReport> report = List.of(new ColumnReport("l", "INTEGER[]", ColumnReport.ARRAY, 0, 0),
                new ColumnReport("j", "JSON", ColumnReport.OTHER, 0, 0));
        List<List<Object>> rows = List.of(
                java.util.Arrays.asList(java.util.Arrays.asList("a\u001f\"", 1L, null, true), "{ \"a\" : 1.50, \"b\": [1e2, -0, \"\\u001f\"] }"),
                java.util.Arrays.asList(java.util.Arrays.asList(1.5, new BigDecimal("1.50")), "[]"));
        List<List<Object>> back = cells(ArrowReader.open(ByteBuffer.wrap(written(columns, report, rows))));
        // a list as Jackson writes an array: upper-case hex for a control character, numbers as Jackson spells them
        assertEquals(List.of("[\"a\\u001F\\\"\",1,null,true]", "[1.5,1.50]"), back.get(0));
        // a JSON column re-spelled as Jackson writes the tree it reads: compact, a double Double.toString's
        assertEquals(List.of("{\"a\":1.5,\"b\":[100.0,0,\"\\u001F\"]}", "[]"), back.get(1));
        // a list within a list: upstream fails on it (Jackson cannot serialize the driver's nested array), so lite refuses
        IllegalStateException nested = assertThrows(IllegalStateException.class, () -> written(List.of(col("l", "String")),
                List.of(new ColumnReport("l", "INTEGER[][]", ColumnReport.ARRAY, 0, 0)),
                List.of(List.of(List.of(List.of(1L))))));
        assertTrue(nested.getMessage().contains("a list within a list"), nested.getMessage());
    }

    @Test
    void valuesAreSetAsUpstreamSetsThem() throws IOException {
        List<ExecutionPlan.Column> columns = List.of(col("n", "Integer"), col("f", "Float"), col("d", "Decimal"),
                col("t", "DateTime"), col("day", "StrictDate"), col("sum", "Integer"), col("s", "String"));
        List<ColumnReport> report = List.of(report("N", ColumnReport.INTEGER, 10, 0),
                report("F", ColumnReport.FLOAT, 0, 0), report("D", ColumnReport.DECIMAL, 10, 2),
                report("T", ColumnReport.TIMESTAMP, 0, 0), report("DAY", ColumnReport.DATE, 0, 0),
                report("SUM", ColumnReport.OTHER, 0, 0), report("S", ColumnReport.VARCHAR, 0, 0));
        List<List<Object>> rows = new ArrayList<>();
        rows.add(java.util.Arrays.asList(42L, 0.1, new BigDecimal("12.345"),
                new PureDateLiteral.DateWithSubsecond(2024, 1, 2, 3, 4, 5, "123456789"),
                new PureDateLiteral.StrictDate(2024, 1, 31), new BigInteger("9007199254740993"), "é"));
        rows.add(java.util.Arrays.asList(-1L, -2.5, new BigDecimal("-12.345"),
                new PureDateLiteral.DateWithSubsecond(1969, 12, 31, 23, 59, 59, "999999"),
                new PureDateLiteral.StrictDate(1969, 12, 31), 7L, null));
        rows.add(java.util.Arrays.asList(null, null, new BigDecimal("12.344"), null, null, null, null));
        ArrowReader.Stream read = ArrowReader.open(ByteBuffer.wrap(written(columns, report, rows)));
        ArrowReader.Schema schema = read.schema();
        assertEquals(List.of("N", "F", "D", "T", "DAY", "SUM", "S"), schema.fields().stream().map(ArrowField::name).toList(),
                "each field named by its label");
        assertTrue(schema.fields().stream().allMatch(f -> f.nullable() && f.metadata().isEmpty()));
        List<List<Object>> back = cells(read);
        assertEquals(List.of(42L, -1L, "null"), back.get(0));
        assertEquals(List.of((double) 0.1f, -2.5, "null"), back.get(1), "a FLOAT's 32 bits");
        // HALF_UP at the reported scale
        assertEquals(List.of("12.35", "-12.35", "12.34"), back.get(2));
        // microseconds, finer digits dropped (and before the epoch)
        long jan2 = java.time.LocalDateTime.of(2024, 1, 2, 3, 4, 5).toEpochSecond(java.time.ZoneOffset.UTC);
        assertEquals(List.of(jan2 * 1_000_000L + 123_456L, -1L, "null"), back.get(3));
        assertEquals(List.of((long) java.time.LocalDate.of(2024, 1, 31).toEpochDay(), -1L, "null"), back.get(4));
        assertEquals(List.of("9007199254740993", "7", "null"), back.get(5), "a sum of integers its digits");
        assertEquals(List.of("é", "null", "null"), back.get(6));
    }

    @Test
    void aTimestampBefore1582IsRefusedByName() throws IOException {
        // upstream writes it through JDBC's Timestamp, on its Julian-hybrid calendar (days from the stored date): the frozen
        // route refuses it rather than write it on another calendar (register S136; D21, 2026-11-03)
        List<ExecutionPlan.Column> columns = List.of(col("t", "DateTime"));
        List<ColumnReport> report = List.of(report("T", ColumnReport.TIMESTAMP, 0, 0));
        IllegalStateException before = assertThrows(IllegalStateException.class, () -> written(columns, report,
                List.of(List.of(new PureDateLiteral.DateWithSecond(1582, 10, 14, 23, 59, 59)))));
        assertTrue(before.getMessage().contains("column 'T': the timestamp 1582-10-14 is before 1582-10-15"),
                before.getMessage());
        // the first day of the Gregorian calendar is written as every later one
        List<List<Object>> back = cells(ArrowReader.open(ByteBuffer.wrap(written(columns, report,
                List.of(List.of(new PureDateLiteral.DateWithSecond(1582, 10, 15, 0, 0, 0)))))));
        assertEquals(List.of(java.time.LocalDateTime.of(1582, 10, 15, 0, 0).toEpochSecond(java.time.ZoneOffset.UTC)
                * 1_000_000L), back.get(0));
    }

    @Test
    void batchesOf1024RowsAndNoneForAnEmptyResult() throws IOException {
        List<ExecutionPlan.Column> columns = List.of(col("n", "Integer"));
        List<ColumnReport> report = List.of(report("n", ColumnReport.BIGINT, 19, 0));
        List<List<Object>> rows = new ArrayList<>();
        for (long i = 0; i < 2500; i++) {
            rows.add(List.of(i));
        }
        ArrowReader.Stream read = ArrowReader.open(ByteBuffer.wrap(written(columns, report, rows)));
        List<Integer> sizes = new ArrayList<>();
        for (ArrowReader.Batch b = read.next(); b != null; b = read.next()) {
            sizes.add(b.rows());
        }
        assertEquals(List.of(1024, 1024, 452), sizes);
        ArrowReader.Stream none = ArrowReader.open(ByteBuffer.wrap(written(columns, report, List.of())));
        assertEquals(null, none.next(), "no record batch for an empty result");
    }

    @Test
    void withoutTheDatabasesReportItRefuses() {
        List<ExecutionPlan.Column> columns = List.of(col("n", "Integer"));
        EngineArrowWriter w = new EngineArrowWriter(new ByteArrayOutputStream(), Map.of());
        IllegalStateException none = assertThrows(IllegalStateException.class, () -> w.columns(columns));
        assertTrue(none.getMessage().contains("typed from the database's report"), none.getMessage());
        EngineArrowWriter two = new EngineArrowWriter(new ByteArrayOutputStream(), Map.of());
        two.reported(List.of(report("a", ColumnReport.INTEGER, 0, 0), report("b", ColumnReport.INTEGER, 0, 0)));
        IllegalStateException count = assertThrows(IllegalStateException.class, () -> two.columns(columns));
        assertTrue(count.getMessage().contains("a mixed column"), count.getMessage());
    }

    private static byte[] written(List<ExecutionPlan.Column> columns, List<ColumnReport> report, List<List<Object>> rows)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        EngineArrowWriter w = new EngineArrowWriter(out, Map.of("legend.builder", "{}"));
        w.reported(report);
        w.columns(columns);
        // in batches of 700 (not upstream's size): the writer re-batches to 1,024
        for (int from = 0; from < rows.size(); from += 700) {
            Batch.Builder b = Batch.builder(columns, 700);
            for (List<Object> row : rows.subList(from, Math.min(rows.size(), from + 700))) {
                b.add(row);
            }
            w.batch(b.build());
        }
        w.end();
        return out.toByteArray();
    }

    /** Each column's values over every batch: a whole number or a day as a long, a float as a double, a decimal its
     *  plain digits, a text as it is, a null "null". */
    private static List<List<Object>> cells(ArrowReader.Stream read) throws IOException {
        List<List<Object>> out = new ArrayList<>();
        for (ArrowReader.Batch b = read.next(); b != null; b = read.next()) {
            for (int c = 0; c < b.columns().size(); c++) {
                if (out.size() <= c) {
                    out.add(new ArrayList<>());
                }
                ArrowReader.Array a = b.columns().get(c);
                for (int r = 0; r < b.rows(); r++) {
                    out.get(c).add(a.isNull(r) ? "null" : value(a, r));
                }
            }
        }
        return out;
    }

    private static Object value(ArrowReader.Array a, int r) {
        ByteBuffer v = a.buffer(0);
        return switch (a.field().type()) {
            case ArrowType.Int i -> i.bits() == 32 ? (long) v.getInt(4 * r) : v.getLong(8 * r);
            case ArrowType.FloatingPoint f -> f.precision() == ArrowType.FloatPrecision.SINGLE ? (double) v.getFloat(4 * r)
                    : v.getDouble(8 * r);
            case ArrowType.Decimal d -> {
                long low = v.getLong(16 * r);
                long high = v.getLong(16 * r + 8);
                BigInteger unscaled = BigInteger.valueOf(high).shiftLeft(64).add(new BigInteger(Long.toUnsignedString(low)));
                yield new BigDecimal(unscaled, d.scale()).toPlainString();
            }
            case ArrowType.Timestamp t -> v.getLong(8 * r);
            case ArrowType.Date d -> (long) v.getInt(4 * r);
            case ArrowType.Utf8 u -> {
                ByteBuffer data = a.buffer(1);
                int start = v.getInt(4 * r);
                int end = v.getInt(4 * r + 4);
                byte[] bytes = new byte[end - start];
                data.duplicate().position(start).get(bytes);
                yield new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            }
            default -> throw new IllegalStateException("not read here: " + a.field().type());
        };
    }
}
