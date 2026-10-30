// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.arrowipc.ZstdFrame;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testing.Runfile;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE ROUND-TRIP GATE of lite's Arrow writer (the result boundary program's track A, 2026-10-29): a batch of every
 * kind written by {@link ArrowWriter} and by {@link JsonRowsWriter}, pyarrow (a standard Arrow reader, test-only:
 * src/test/python/arrow_matches_rows.py, the py_binary //core:arrow_matches_rows on the hermetic interpreter with
 * the locked pyarrow) reading the stream, each value spelled by Pure's rules and compared with the JSON cell; the
 * stream's zstd frame read back by pyarrow's zstd. The writer's refusals, by name.
 */
@Tag("arrow")
class ArrowWriterTest {

    private static ExecutionPlan.Column col(String name, String type, String slot, ExecutionPlan.Precision p) {
        boolean many = slot != null && slot.startsWith("ARRAY<");
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(0, many ? null : 1), slot, null, p);
    }

    private static Batch batch(List<ExecutionPlan.Column> columns, List<List<Object>> rows) {
        Batch.Builder b = Batch.builder(columns, Math.max(1, rows.size()));
        for (List<Object> r : rows) {
            b.add(r);
        }
        return b.build();
    }

    /** Both writers over the same columns and batches; the comparator's verdict. */
    private static void matches(List<ExecutionPlan.Column> columns, List<Batch> batches) throws Exception {
        ByteArrayOutputStream arrow = new ByteArrayOutputStream();
        StringWriter json = new StringWriter();
        ArrowWriter aw = new ArrowWriter(arrow, Map.of("legend.test", "round trip"));
        JsonRowsWriter jw = new JsonRowsWriter(json, ExecutionPlan.TdsResult.Shape.RELATION);
        aw.columns(columns);
        jw.columns(columns);
        for (Batch b : batches) {
            aw.batch(b);
            jw.batch(b);
        }
        aw.end();
        jw.end();
        Path dir = Files.createTempDirectory("arrow-vs-rows");
        Files.write(dir.resolve("stream.arrows"), arrow.toByteArray());
        Files.writeString(dir.resolve("rows.json"), json.toString(), StandardCharsets.UTF_8);
        Files.write(dir.resolve("frame.zst"), ZstdFrame.of(arrow.toByteArray()));
        ProcessBuilder comparator = new ProcessBuilder(Runfile.property("core.arrow.comparator").toString(),
                dir.toString()).redirectErrorStream(true);
        comparator.environment().putAll(Runfile.env());
        Process p = comparator.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(120, TimeUnit.SECONDS), "the comparator did not finish");
        assertEquals(0, p.exitValue(), out);
    }

    private static List<Object> row(Object... cells) {
        return Arrays.asList(cells);
    }

    @Test
    void everyKindCarriesTheSameValuesAsItsJsonRows() throws Exception {
        List<ExecutionPlan.Column> columns = List.of(
                col("i", "Integer", "BIGINT", null),
                col("h", "Integer", "HUGEINT", null),
                col("f", "Float", "DOUBLE", null),
                col("d", "Decimal", "DECIMAL(10,2)", null),
                col("dt", "Decimal", "DECIMAL_TEXT", null),
                col("b", "Boolean", "BOOLEAN", null),
                col("s", "String", "VARCHAR", null),
                col("day", "StrictDate", "DATE", ExecutionPlan.Precision.DAY),
                col("at", "DateTime", "TIMESTAMP", ExecutionPlan.Precision.INSTANT),
                col("written", "Date", "TEMPORAL_TEXT", ExecutionPlan.Precision.WRITTEN),
                col("t", "StrictTime", "TIME", null),
                col("l", "Integer", "ARRAY<BIGINT>", null),
                col("st", "Any", "STRUCT<x BIGINT, y VARCHAR>", null),
                col("m", "Any", "MAP<VARCHAR,BIGINT>", null));
        List<Object> first = row(1L, new BigInteger("99999999999999999999999999999999999999"), 1.5, new BigDecimal("12.3"),
                new BigDecimal("0.000001"), true, "héllo, \"quoted\"", PureDateLiteral.parse("2024-02-29"),
                PureDateLiteral.parse("2024-02-29T13:14:15.123456789"), PureDateLiteral.parse("2024-02"),
                PureTimeLiteral.parse("13:14:15.5"), List.of(1L, 2L, 3L), structOf(7L, "y"), mapOf("k", 9L));
        List<Object> nulls = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            nulls.add(null);
        }
        List<Object> third = row(-9223372036854775808L, BigInteger.valueOf(-5), -0.0, new BigDecimal("-0.01"),
                new BigDecimal("123456789012345678901234567890.5"), false, "", PureDateLiteral.parse("1969-12-31"),
                PureDateLiteral.parse("1969-12-31T23:59:59"), PureDateLiteral.parse("2024-02-29T10:00+0000"),
                PureTimeLiteral.parse("00:00"), List.of(), structOf(null, null), mapOf());
        matches(columns, List.of(batch(columns, List.of(first, nulls)), batch(columns, List.of(third))));
    }

    @Test
    void aTaggedColumnIsAUnionOfItsSlots() throws Exception {
        // a mixed value's column (TAGGED<kind slot, ...>): each cell in its slot's member, by the carrier it is
        List<ExecutionPlan.Column> columns = List.of(
                col("v", "Any", "TAGGED<Integer BIGINT, String VARCHAR, StrictDate DATE>", null),
                col("n", "Integer", "BIGINT", null));
        matches(columns, List.of(batch(columns, List.of(row(new TaggedCell(0, 1L), 1L), row(new TaggedCell(1, "a"), 2L),
                row(null, 3L), row(new TaggedCell(2, PureDateLiteral.parse("2024-01-02")), 4L)))));
    }

    @Test
    void aValueTheColumnCannotHoldIsRefusedByName() throws Exception {
        List<ExecutionPlan.Column> decimal = List.of(col("d", "Decimal", "DECIMAL(10,2)", null));
        ArrowWriter w = new ArrowWriter(new ByteArrayOutputStream(), Map.of());
        w.columns(decimal);
        IllegalStateException more = assertThrows(IllegalStateException.class,
                () -> w.batch(batch(decimal, List.of(row(new BigDecimal("1.234"))))));
        assertTrue(more.getMessage().contains("never rounded"), more.getMessage());
        // a HUGEINT past decimal128(38, 0)'s 38 digits (DuckDB's HUGEINT reaches 39): refused by name, never truncated
        List<ExecutionPlan.Column> huge = List.of(col("h", "Integer", "HUGEINT", null));
        ArrowWriter w4 = new ArrowWriter(new ByteArrayOutputStream(), Map.of());
        w4.columns(huge);
        IllegalStateException digits = assertThrows(IllegalStateException.class,
                () -> w4.batch(batch(huge, List.of(row(new BigInteger("170141183460469231731687303715884105727"))))));
        assertTrue(digits.getMessage().contains("more digits"), digits.getMessage());

        List<ExecutionPlan.Column> instant = List.of(col("at", "DateTime", "TIMESTAMP", ExecutionPlan.Precision.INSTANT));
        ArrowWriter w2 = new ArrowWriter(new ByteArrayOutputStream(), Map.of());
        w2.columns(instant);
        IllegalStateException day = assertThrows(IllegalStateException.class,
                () -> w2.batch(batch(instant, List.of(row(PureDateLiteral.parse("2024-01-02"))))));
        assertTrue(day.getMessage().contains("time of day"), day.getMessage());

        // a bare carrier under a tagged column: the batch refuses it (the tag rides in a TaggedCell)
        List<ExecutionPlan.Column> tagged = List.of(col("v", "Any", "TAGGED<String VARCHAR, s::Colour VARCHAR>", null));
        IllegalArgumentException bare = assertThrows(IllegalArgumentException.class,
                () -> batch(tagged, List.of(row("RED"))));
        assertTrue(bare.getMessage().contains("TaggedCell"), bare.getMessage());
    }

    @Test
    void aNonNumericTypeOverANumericSlotIsItsText() throws Exception {
        // the decoders' rule: a String-typed property over an INT column carries the column's own text (the track A
        // audit, finding 3: typed by the slot alone, the Arrow route refused the text)
        List<ExecutionPlan.Column> columns = List.of(col("id", "String", "INTEGER", null),
                col("n", "Integer", "BIGINT", null));
        matches(columns, List.of(batch(columns, List.of(row("7", 1L), row(null, 2L), row("x9", 3L)))));
    }

    @Test
    void aUnionsMembersOfEveryKindReadBack() throws Exception {
        // a decimal, a timestamp and a double beside an integer (the track A audit, finding 16: a union read at the
        // member's own precision -- a timestamp's nanoseconds, a decimal's scale)
        List<ExecutionPlan.Column> columns = List.of(
                col("v", "Any", "TAGGED<Integer BIGINT, Decimal DECIMAL(10,2), DateTime TIMESTAMP, Float DOUBLE>", null));
        matches(columns, List.of(batch(columns, List.of(row(new TaggedCell(0, 1L)),
                row(new TaggedCell(1, new BigDecimal("2.50"))),
                row(new TaggedCell(2, PureDateLiteral.parse("2024-02-29T13:14:15.123456789"))),
                row(new TaggedCell(3, 1.5)), row((Object) null)))));
    }

    @Test
    void twoSlotsOfOneCarrierReadByTheirTags() throws Exception {
        // the tag in the batch (track F, 2026-10-29; the track A audit's finding 5): two VARCHAR slots (an enumeration
        // beside a String) and two TIMESTAMP slots (a mixed-precision DateTime list, D53 (2)), each cell in the member
        // its tag names -- the JSON route spells the carrier as before
        List<ExecutionPlan.Column> texts = List.of(col("v", "Any", "TAGGED<String VARCHAR, s::Colour VARCHAR>", null));
        matches(texts, List.of(batch(texts, List.of(row(new TaggedCell(1, "RED")), row(new TaggedCell(0, "red")),
                row((Object) null)))));
        List<ExecutionPlan.Column> dates = List.of(
                col("d", "Any", "TAGGED<DateTime(3) TIMESTAMP, DateTime TIMESTAMP>", null));
        matches(dates, List.of(batch(dates, List.of(
                row(new TaggedCell(0, PureDateLiteral.parse("2020-01-01T10:00:00.123"))),
                row(new TaggedCell(1, PureDateLiteral.parse("2020-01-01T10:00:00")))))));
    }

    @Test
    void aMapsNullValueAndAScaleZeroDecimalAndAnObject() throws Exception {
        // a map's value may be null (its key never); DECIMAL(p) is SQL's scale of zero; a class-typed column's object
        // rides as the JSON route writes it (the track A audit, findings 11, 16 and 18)
        List<ExecutionPlan.Column> columns = List.of(
                col("m", "Any", "MAP<VARCHAR,BIGINT>", null),
                col("d10", "Decimal", "DECIMAL(10)", null),
                col("p", "my::Person", null, null));
        Map<String, Object> person = new LinkedHashMap<>();
        person.put("name", "x");
        person.put("age", 3L);
        matches(columns, List.of(batch(columns, List.of(
                row(mapOf("k", null, "j", 2L), new BigDecimal("12"), person),
                row(mapOf(), new BigDecimal("-3"), null)))));
    }

    @Test
    void aTenthSubsecondDigitIsRefusedNeverTruncated() {
        List<ExecutionPlan.Column> instant = List.of(col("at", "DateTime", "TIMESTAMP", ExecutionPlan.Precision.INSTANT));
        ArrowWriter w = new ArrowWriter(new ByteArrayOutputStream(), Map.of());
        IllegalStateException ten = assertThrows(IllegalStateException.class, () -> {
            w.columns(instant);
            w.batch(batch(instant, List.of(row(PureDateLiteral.parse("2024-01-02T03:04:05.1234567891")))));
        });
        assertTrue(ten.getMessage().contains("never truncated"), ten.getMessage());
    }

    @Test
    void aWholeNumberOfSixtyThreeBitsIsALong() {
        // the decoders' carrier for [2^62, 2^63-1] was a BigInteger (63 exclusive) until the track A audit's finding 2;
        // the Arrow writer's int64 takes a long alone, by design
        List<ExecutionPlan.Column> columns = List.of(col("i", "Integer", "BIGINT", null));
        assertEquals(List.of(Long.MAX_VALUE), RowDecode.cells("{\"i\": 9223372036854775807}", columns));
        assertEquals(List.of(4611686018427387904L), RowDecode.cells("{\"i\": 4611686018427387904}", columns));
        assertEquals(List.of(Long.MIN_VALUE), RowDecode.cells("{\"i\": -9223372036854775808}", columns));
        assertEquals(List.of(new BigInteger("9223372036854775808")),
                RowDecode.cells("{\"i\": 9223372036854775808}", columns));
    }

    @Test
    void theZstdFrameHoldsTheStreamAndNothingElse() {
        byte[] content = new byte[300_000];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i * 31);
        }
        byte[] frame = ZstdFrame.of(content);
        assertEquals(0x28, frame[0] & 0xFF);
        assertEquals(0xB5, frame[1] & 0xFF);
        // the header: magic (4), descriptor (1), window (1), content size (8); then three blocks of 3-byte headers
        assertEquals(4 + 1 + 1 + 8 + 3 * 3 + content.length, frame.length);
        assertEquals(1, frame[frame.length - (content.length - 2 * 131072) - 3] & 1, "the last block is flagged");
    }

    private static Map<String, Object> structOf(Object x, Object y) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("x", x);
        m.put("y", y);
        return m;
    }

    private static Map<Object, Object> mapOf(Object... kv) {
        Map<Object, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }
}
