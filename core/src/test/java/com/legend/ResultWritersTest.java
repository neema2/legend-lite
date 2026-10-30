// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.ExecutionPlan.Column;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.executionplan.ExecutionPlan.TdsResult.Shape;
import com.legend.json.Json;
import com.legend.result.Batcher;
import com.legend.result.CellText;
import com.legend.result.CsvWriter;
import com.legend.result.JsonRowsWriter;
import com.legend.result.PureV1Writer;
import com.legend.result.ResultSink;
import com.legend.values.PureDateLiteral;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The writers at the edge (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, phase 2 of the end state recorded 2026-10-27):
 *  each typed cell's text by the column's facts alone, in Pure's print forms and in legend-engine's tokens. */
class ResultWritersTest {

    private static final Multiplicity ONE = new Multiplicity(1, 1);

    /** A column with no temporal of its own (its written-precision fact null, said). */
    private static Column col(String name, String type, String slot) {
        return new Column(name, type, ONE, slot, null, null);
    }

    /** A temporal column with its written-precision fact, as ColumnFacts.precision sets it for that type and slot. */
    private static Column col(String name, String type, String slot, ExecutionPlan.Precision precision) {
        return new Column(name, type, ONE, slot, null, precision);
    }

    private static String write(ResultSink sink, StringWriter out, List<Column> columns, List<List<?>> rows)
            throws IOException {
        Batcher b = new Batcher(columns, sink, 2);
        b.start();
        for (List<?> r : rows) {
            b.row(r);
        }
        b.end();
        return out.toString();
    }

    // ---- Pure's text ------------------------------------------------------------------------------------------------

    @Test
    void aFloatPrintsItsShortestDigitsWithAPoint_neverAnExponent() {
        Column f = col("f", "Float", "DOUBLE");
        assertEquals("1500000000000000.0", CellText.pure(1.5e15, f));
        assertEquals("0.00000025", CellText.pure(2.5e-7, f));
        assertEquals("-0.0", CellText.pure(-0.0, f));
        assertEquals("2.0", CellText.pure(new BigDecimal("2.000"), f));
        // the double nearest 123456789123456789.99 prints its shortest digits (Pure's print of the double, the numeric
        // charter's measurement), not its binary expansion
        assertEquals("123456789123456780.0", CellText.pure(123456789123456789.99, f));
        // a double below 1e-3 whose shortest digits are one digit: the exponent form's mantissa trailing zero (1.0E-4)
        // is no digit of the value, so the text is legend-pure's own (CompiledSupport.primitiveToString's DecimalFormat,
        // measured 2026-10-28, docs/result-boundary-homework/probes/FloatTextProbe.java.txt), never 0.00010 (the
        // user, 2026-10-28: a defect; the start page's S1.4 item 2)
        assertEquals("0.0001", CellText.pure(1e-4, f));
        assertEquals("0.00001", CellText.pure(1e-5, f));
        assertEquals("0.00002", CellText.pure(2e-5, f));
        assertEquals("0.000012345", CellText.pure(1.2345e-5, f));
        assertEquals("-0.0001", CellText.pure(-1e-4, f));
        assertEquals("10000000.0", CellText.pure(1e7, f));
        assertEquals("0.001", CellText.pure(0.001, f));
        assertEquals("0.1", CellText.pure(0.1, f));
        assertEquals("0.3333333333333333", CellText.pure(1.0 / 3, f));
        // lite's own JSON carries the same text as its number token
        assertEquals("0.0001", Json.toCompact(CellText.pureJson(1e-4, f)));
        // legend-engine's token is Java's own, exponent and all: untouched
        assertEquals("1.0E-4", Json.toCompact(CellText.engine(1e-4, f)));
    }

    /** A negative zero is another double, so lite's own outputs keep its sign: the shortest text that reads back as
     *  the same double (the user, 2026-10-28; register S117). A decimal has no negative zero. */
    @Test
    void aNegativeZeroKeepsItsSign_inLitesCsvAndJson() {
        Column f = col("f", "Float", "DOUBLE");
        assertEquals("-0.0", CellText.pure(-0.0, f));
        assertEquals("0.0", CellText.pure(0.0, f));
        assertEquals("-0.0", Json.toCompact(CellText.pureJson(-0.0, f)));
        assertEquals("0.0", Json.toCompact(CellText.pureJson(0.0, f)));
        assertEquals("0.0", CellText.pure(new BigDecimal("-0.000"), f));
        // legend-engine's JSON writes the double as Java does, its sign too
        assertEquals("-0.0", Json.toCompact(CellText.engine(-0.0, f)));
    }

    /** legend-engine writes a database timestamp in full and only a database date as its day (its ValueTransformer;
     *  legend-pure reads a SQL timestamp with its nine digits, DateFunctions.fromSQLTimestamp, and prints them), so a
     *  Date at midnight in a relation's TIMESTAMP column is a date-time like its others in lite's own outputs too
     *  (2026-10-29, the upstream-fidelity tenet: the writer's midnight rule of 12b89c7d, the day alone, is gone); a
     *  value root's instant is written at the precision its digits show (MINIMAL, register S118). */
    @Test
    void aTimestampCellSpellsItsNineDigits_evenAtMidnight_aValueRootItsMinimalDigits() {
        Column date = col("d", "Date", "TIMESTAMP", ExecutionPlan.Precision.INSTANT);
        PureDateLiteral midnight = PureDateLiteral.fromDatabaseTimestamp(java.time.LocalDateTime.of(2026, 1, 7, 0, 0));
        assertEquals("\"2026-01-07T00:00:00.000000000+0000\"", Json.toCompact(CellText.engine(midnight, date)));
        assertEquals("2026-01-07T00:00:00.000000000+0000", CellText.pure(midnight, date));
        assertEquals("\"2026-01-07T00:00:00.000000000+0000\"", Json.toCompact(CellText.pureJson(midnight, date)));
        assertEquals("2026-01-07T00:00:00+0000",
                CellText.pure(midnight, col("v", "Date", "TIMESTAMP", ExecutionPlan.Precision.MINIMAL)));
        assertEquals("\"2026-01-07\"", Json.toCompact(CellText.engine(PureDateLiteral.parse("2026-01-07"),
                col("s", "StrictDate", "DATE", ExecutionPlan.Precision.DAY))));
    }

    @Test
    void aDecimalKeepsItsScale_anIntegerPastALongItsDigits() {
        assertEquals("1.50", CellText.pure(new BigDecimal("1.50"), col("d", "Decimal", "DECIMAL(10,2)")));
        assertEquals("18446744073709551614",
                CellText.pure(new BigInteger("18446744073709551614"), col("i", "Integer", "HUGEINT")));
    }

    @Test
    void aDateTimePrintsAtItsPrecisionWithItsZone_aStrictDateItsDay() {
        // a relation's TIMESTAMP cell: the nine digits the database handed over, as legend-engine and legend-pure
        // spell a SQL timestamp (2026-10-29); a value root's: the precision its digits show (MINIMAL, S118)
        PureDateLiteral half = PureDateLiteral.fromDatabaseTimestamp(java.time.LocalDateTime.of(2024, 1, 2, 10, 30, 0,
                500_000_000));
        assertEquals("2024-01-02T10:30:00.500000000+0000",
                CellText.pure(half, col("t", "DateTime", "TIMESTAMP", ExecutionPlan.Precision.INSTANT)));
        assertEquals("2024-01-02T10:30:00.5+0000",
                CellText.pure(half, col("t", "DateTime", "TIMESTAMP", ExecutionPlan.Precision.MINIMAL)));
        assertEquals("2024-01-02", CellText.pure(PureDateLiteral.parse("2024-01-02"),
                col("d", "StrictDate", "DATE", ExecutionPlan.Precision.DAY)));
        // a value root's cell carried as its own text keeps the precision it was written at: the plan's fact
        // (2026-10-28, S1.4 item 1), where the writer matched the slot's spelling until then; a relation's cell carried
        // the same way spells as legend-engine's TDS does, nine digits (INSTANT, 2026-10-29: ColumnFacts.precision gives
        // the fact by the result's shape)
        assertEquals("2024-01-02T10:30+0000", CellText.pure(PureDateLiteral.parse("2024-01-02T10:30"),
                col("t", "DateTime", "TEMPORAL_TEXT", ExecutionPlan.Precision.WRITTEN)));
        // an hour-only date has no zone: legend-pure's DateFormat.append writes +0000 from a minute on (S1.5, G5)
        assertEquals("2024-01-02T10", CellText.pure(PureDateLiteral.parse("2024-01-02T10"),
                col("t", "DateTime", "TEMPORAL_TEXT", ExecutionPlan.Precision.WRITTEN)));
        assertEquals("\"2024-01-02T10\"", Json.toCompact(CellText.pureJson(PureDateLiteral.parse("2024-01-02T10"),
                col("t", "DateTime", "TEMPORAL_TEXT", ExecutionPlan.Precision.WRITTEN))));
        assertEquals("2024-01-02T10:30:00.000000000+0000", CellText.pure(PureDateLiteral.parse("2024-01-02T10:30"),
                col("t", "DateTime", "TEMPORAL_TEXT", ExecutionPlan.Precision.INSTANT)));
        // a DateTime written as a day in a DATE slot is a day: the fact, not the slot's word
        assertEquals("2024-01-02", CellText.pure(PureDateLiteral.parse("2024-01-02T00:00:00"),
                col("d", "DateTime", "DATE", ExecutionPlan.Precision.DAY)));
        // a date-time of a mixed column (its fact none: a tagged winner's) spells by the value's own precision
        assertEquals("2024-01-02T10:30:00+0000", CellText.pure(PureDateLiteral.parse("2024-01-02T10:30:00"),
                col("m", "Any", "TAGGED<DateTime TIMESTAMP, String VARCHAR>")));
    }

    @Test
    void structuresPrintAsThePreludeSays() {
        assertEquals("[2, 3]", CellText.pure(List.of(2L, 3L), col("v", "List<Integer>", "ARRAY<BIGINT>")));
        Map<String, Object> pair = new LinkedHashMap<>();
        pair.put("first", 1L);
        pair.put("second", "a");
        assertEquals("<1, a>", CellText.pure(pair, col("v", "Pair<Integer, String>", "STRUCT<first BIGINT>")));
        Map<String, Object> outer = new LinkedHashMap<>();
        outer.put("first", pair);
        outer.put("second", 4L);
        assertEquals("<<1, a>, 4>",
                CellText.pure(outer, col("v", "Pair<Pair<Integer, String>, Integer>", "STRUCT<first STRUCT>")));
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("key1", 0L);
        map.put("key2", 1L);
        assertEquals("{key1=0, key2=1}", CellText.pure(map, col("v", "Map<String, Integer>", "MAP<VARCHAR,BIGINT>")));
        Map<String, Object> person = new LinkedHashMap<>();
        person.put("name", "Ann");
        person.put("__id", "o1");
        person.put("__type", "model::Person");
        assertEquals("@_o1", CellText.pure(person, col("v", "model::Person", "STRUCT<name VARCHAR>")));
    }

    @Test
    void aStructureOfNoKnownPrintIsRefused() {
        var refused = assertThrows(IllegalArgumentException.class,
                () -> CellText.pure(Map.of("name", "Ann"), col("v", "model::Person", "STRUCT<name VARCHAR>")));
        assertTrue(refused.getMessage().contains("Pure's text"), refused.getMessage());
    }

    // ---- legend-engine's tokens -------------------------------------------------------------------------------------

    @Test
    void engineTokens() {
        assertEquals("1.0E15", Json.toCompact(CellText.engine(1.0e15, col("f", "Float", "DOUBLE"))));
        // a NaN or infinite Float cell is null: the engine's relational JSON (convertPurePrimitiveToJson), read
        // 2026-10-29 (S1.5); phase 2 had pinned the name as a string
        assertEquals("null", Json.toCompact(CellText.engine(Double.NaN, col("f", "Float", "DOUBLE"))));
        assertEquals("null", Json.toCompact(CellText.engine(Double.POSITIVE_INFINITY, col("f", "Float", "DOUBLE"))));
        assertEquals("1.50", Json.toCompact(CellText.engine(new BigDecimal("1.50"), col("d", "Decimal", "DECIMAL"))));
        // a whole number past a long is written by its digits, never wrapped
        assertEquals("18446744073709551614", Json.toCompact(
                CellText.engine(new BigInteger("18446744073709551614"), col("i", "Integer", "HUGEINT"))));
        assertEquals("\"2024-01-02T10:30:00.500000000+0000\"", Json.toCompact(CellText.engine(
                PureDateLiteral.parse("2024-01-02T10:30:00.5"), col("t", "DateTime", "TIMESTAMP",
                        ExecutionPlan.Precision.INSTANT))));
        Map<String, Object> person = new LinkedHashMap<>();
        person.put("name", "Ann");
        person.put("__id", "o1");
        assertEquals("{\"name\":\"Ann\"}", Json.toCompact(CellText.engine(person, col("v", "model::Person", "JSON"))));
        // a Variant is a string holding its JSON, its JSON null too: the value's text, never JSON's null, which is a
        // NULL cell's (measured, 4.145.0, 2026-10-28: PARK-34's probe, upstream-api/e8-variant-null.json)
        assertEquals("\"null\"", Json.toCompact(CellText.engine("null",
                col("v", "meta::pure::metamodel::variant::Variant", "JSON"))));
        assertEquals("\"{\\\"a\\\":[1,2.50]}\"", Json.toCompact(CellText.engine("{\"a\":[1,2.50]}",
                col("v", "meta::pure::metamodel::variant::Variant", "JSON"))));
    }

    /** A structure's values are written by the rules of cells of their own kinds -- a nested date, time or float as
     *  one at the top is -- in both JSON forms (the phase 3 audit, 2026-10-27: a nested date threw "Cannot coerce to
     *  JSON node"). */
    @Test
    void aStructuresDatesTimesAndFloatsAreWrittenAsCellsOfTheirKinds() {
        Map<String, Object> person = new LinkedHashMap<>();
        person.put("when", PureDateLiteral.parse("2024-01-02"));
        person.put("at", PureDateLiteral.parse("2024-01-02T10:30:00.500"));
        person.put("tick", com.legend.values.PureTimeLiteral.parse("10:30:00"));
        person.put("f", 1.0e15);
        person.put("tags", List.of(PureDateLiteral.parse("2024-01-03")));
        person.put("__id", "o1");
        Column c = col("v", "model::Person", "STRUCT<when DATE>");
        assertEquals("{\"when\":\"2024-01-02\",\"at\":\"2024-01-02T10:30:00.500000000+0000\",\"tick\":\"10:30:00\","
                + "\"f\":1.0E15,\"tags\":[\"2024-01-03\"]}", Json.toCompact(CellText.engine(person, c)));
        assertEquals("{\"when\":\"2024-01-02\",\"at\":\"2024-01-02T10:30:00.5+0000\",\"tick\":\"10:30:00\","
                + "\"f\":1000000000000000.0,\"tags\":[\"2024-01-03\"]}", Json.toCompact(CellText.pureJson(person, c)));
    }

    // ---- the writers ------------------------------------------------------------------------------------------------

    private static final List<Column> ROWS = List.of(col("ID", "Integer", "BIGINT"), col("NAME", "String", "VARCHAR"),
            col("F", "Float", "DOUBLE"));

    private static final List<List<?>> DATA = List.of(List.of(1L, "a,b", 2.0), Arrays.asList(2L, null, 0.5),
            List.of(3L, "say \"hi\"", 1e15));

    @Test
    void theCsvIsRfc4180_aNullEmpty_everyLineEndedByCrlf() throws IOException {
        StringWriter out = new StringWriter();
        assertEquals("ID,NAME,F\r\n1,\"a,b\",2.0\r\n2,,0.5\r\n3,\"say \"\"hi\"\"\",1000000000000000.0\r\n",
                write(new CsvWriter(out, Shape.RELATION), out, ROWS, DATA));
    }

    @Test
    void aCsvOfNoRowIsItsHeader_aPivotsGeneratedNameQuoted() throws IOException {
        StringWriter out = new StringWriter();
        assertEquals("'a__|__b',x\r\n", write(new CsvWriter(out, Shape.RELATION), out,
                List.of(col("a__|__b", "Integer", "BIGINT"), col("x", "String", "VARCHAR")), List.of()));
    }

    @Test
    void aGraphFetchHasNoCsv() {
        assertThrows(com.legend.error.NotImplementedException.class,
                () -> new CsvWriter(new StringWriter(), Shape.OBJECTS));
    }

    @Test
    void theJsonRowsArePureTokens() throws IOException {
        StringWriter out = new StringWriter();
        assertEquals("[{\"ID\":1,\"NAME\":\"a,b\",\"F\":2.0},{\"ID\":2,\"NAME\":null,\"F\":0.5},"
                        + "{\"ID\":3,\"NAME\":\"say \\\"hi\\\"\",\"F\":1000000000000000.0}]",
                write(new JsonRowsWriter(out, Shape.RELATION), out, ROWS, DATA));
    }

    @Test
    void thePureV1RelationIsTheEnginesTdsLayout_inTheEnginesColumnOrder() throws IOException {
        StringWriter out = new StringWriter();
        String json = write(PureV1Writer.relation(out, columns -> new PureV1Writer.Envelope(
                "{\"_type\":\"tdsBuilder\"}", "[]", List.of("F", "ID", "NAME"))), out, ROWS, DATA);
        assertEquals("{\"builder\": {\"_type\":\"tdsBuilder\"}, \"activities\": [], \"result\" : {\"columns\" : "
                + "[\"F\",\"ID\",\"NAME\"], \"rows\" : [{\"values\": [2.0,1,\"a,b\"]},{\"values\": [0.5,2,null]},"
                + "{\"values\": [1.0E15,3,\"say \\\"hi\\\"\"]}]}}", json);
        Json.parse(json);
    }

    /** A host's rows (the tab's, Python's) decode by the plan's columns, a structure handed over as its JSON's text
     *  read as JSON by its slot, and write as the server's rows do. */
    @Test
    void aHostsRowsDecodeByThePlansColumns() throws IOException {
        List<Column> columns = List.of(col("ID", "Integer", "BIGINT"), col("NAME", "String", "VARCHAR"),
                col("PRICE", "Decimal", "DECIMAL(10,2)"), col("TAGS", "List<String>", "ARRAY<VARCHAR>"));
        StringWriter out = new StringWriter();
        com.legend.result.HostRows.stream("[{\"ID\":18446744073709551614,\"NAME\":\"[a]\",\"PRICE\":\"1.50\","
                + "\"TAGS\":\"[\\\"x\\\",\\\"y\\\"]\"},{\"ID\":2,\"NAME\":null,\"PRICE\":null,\"TAGS\":[]}]",
                columns, new CsvWriter(out, Shape.RELATION));
        assertEquals("ID,NAME,PRICE,TAGS\r\n18446744073709551614,[a],1.50,\"[x, y]\"\r\n2,,,[]\r\n", out.toString());
        var refused = assertThrows(IllegalArgumentException.class, () -> com.legend.result.HostRows.stream("{}",
                columns, new CsvWriter(new StringWriter(), Shape.RELATION)));
        assertTrue(refused.getMessage().contains("not a JSON array of rows"), refused.getMessage());
        // no rows at all is no answer: refused, never an empty result
        assertThrows(IllegalArgumentException.class, () -> com.legend.result.HostRows.stream("", columns,
                new CsvWriter(new StringWriter(), Shape.RELATION)));
    }

    /** What a host hands as text the server's driver reads typed (the phase 3 audit, 2026-10-27): a Variant's JSON
     *  (its JSON null a value), a structure's nested decimal, a REAL widened to a double -- each read as the server
     *  reads it. */
    @Test
    void aHostsTextIsReadAsTheServerReadsTheValue() throws IOException {
        List<Column> columns = List.of(col("V", "meta::pure::metamodel::variant::Variant", "JSON"),
                col("S", "model::Item", "STRUCT<price DECIMAL(10,2), qty BIGINT>"), col("R", "Float", "REAL"));
        com.legend.result.Collector rows = new com.legend.result.Collector();
        com.legend.result.HostRows.stream("[{\"V\":\"{\\\"a\\\":1}\",\"S\":\"{\\\"price\\\":\\\"1.50\\\",\\\"qty\\\":2}\","
                + "\"R\":0.10000000149011612},{\"V\":\"null\",\"S\":null,\"R\":null}]", columns, rows);
        List<List<Object>> got = rows.collected().rows();
        assertEquals("{\"a\":1}", got.get(0).get(0));
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("price", new BigDecimal("1.50"));
        item.put("qty", 2L);
        assertEquals(item, got.get(0).get(1));
        assertEquals(0.1, got.get(0).get(2));
        assertEquals(Arrays.asList("null", null, null), got.get(1));
    }

    @Test
    void thePureV1ObjectsAreBareWhenOne_anArrayWhenMany() throws IOException {
        List<Column> obj = List.of(col("value", "model::Person", "JSON"));
        StringWriter one = new StringWriter();
        assertEquals("{\"builder\":{\"_type\":\"json\"},\"values\":{\"name\":\"Ann\"}}",
                write(PureV1Writer.objects(one), one, obj, List.of(List.of("{\"name\": \"Ann\"}"))));
        StringWriter many = new StringWriter();
        assertEquals("{\"builder\":{\"_type\":\"json\"},\"values\":[{\"name\":\"Ann\"},{\"name\":\"Bo\"},{\"name\":\"Cy\"}]}",
                write(PureV1Writer.objects(many), many, obj, List.of(List.of("{\"name\":\"Ann\"}"),
                        List.of("{\"name\":\"Bo\"}"), List.of("{\"name\":\"Cy\"}"))));
        StringWriter none = new StringWriter();
        assertEquals("{\"builder\":{\"_type\":\"json\"},\"values\":[]}",
                write(PureV1Writer.objects(none), none, obj, List.of()));
    }
}
