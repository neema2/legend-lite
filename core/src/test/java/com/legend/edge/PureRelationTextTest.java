// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.RowDecode;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * legend-pure's text of a relation in Java ({@link PureRelationText}; stage B's B2, B2b S1, 2026-11-24): each rule
 * pinned to the pinned sources' own examples where they have one (legend-engine 4.145.0's {@code toString.pure} tests,
 * {@code s.pure}, {@code helperFunctions.pure}), and to the edge's SQL composition's rule where B2b replaces it.
 */
class PureRelationTextTest {

    private static ExecutionPlan.Column col(String name, String type) {
        return new ExecutionPlan.Column(name, type, new ExecutionPlan.Multiplicity(0, 1), null, null, null);
    }

    private static List<Object> row(Object... cells) {
        return new ArrayList<>(Arrays.asList(cells));
    }

    // ---- toString.pure's own tests ------------------------------------------------------------------------------

    /** toString.pure's {@code testToString}. */
    @Test
    void theTdsTextOfToStringPuresTest() {
        List<ExecutionPlan.Column> cols = List.of(col("val", "Integer"), col("other", "Integer"), col("txt", "String"));
        List<List<Object>> rows = List.of(row(1L, 2L, "aa"), row(2L, 3L, "ee"), row(3L, 2L, "eewe"));
        assertEquals("#TDS\n   val,other,txt\n   1,2,aa\n   2,3,ee\n   3,2,eewe\n#", PureRelationText.tds(cols, rows));
    }

    @Test
    void noRowsLeavesTheRowsSegmentEmpty() {
        assertEquals("#TDS\n   a\n\n#", PureRelationText.tds(List.of(col("a", "Integer")), List.of()));
    }

    /** toString.pure:27-30: a name trimmed, quoted unless it starts with a quote or is a simple identifier. */
    @Test
    void aNameIsQuotedUnlessSimple() {
        assertEquals("a_1", PureRelationText.tdsName("a_1"));
        assertEquals("'my col'", PureRelationText.tdsName(" my col "));
        assertEquals("'x'", PureRelationText.tdsName("'x'"));
        assertEquals("'a__|__b'", PureRelationText.tdsName("a__|__b"));
    }

    // ---- s.pure: a cell --------------------------------------------------------------------------------------------

    /** s.pure:23-38: an empty cell {@code null}, {@code 'null'} under a Variant; a Variant quoted; a String with a brace
     *  or a bracket quoted, its backslashes and quotes escaped; any other String as it is. The empty cell is the user's
     *  ruling of 2026-11-19 ~18:36 ("Null is good"). */
    @Test
    void aCellIsSPuresText() {
        ExecutionPlan.Column s = col("s", "String");
        ExecutionPlan.Column v = col("v", RowDecode.VARIANT);
        assertEquals("null", PureRelationText.tdsCell(null, s));
        assertEquals("'null'", PureRelationText.tdsCell(null, v));
        assertEquals("'{\"a\":1}'", PureRelationText.tdsCell("{\"a\":1}", v));
        assertEquals("'[1,\\'x\\']'", PureRelationText.tdsCell("[1,'x']", v));
        assertEquals("'{a\\\\b}'", PureRelationText.tdsCell("{a\\b}", s));
        assertEquals("'[it\\'s]'", PureRelationText.tdsCell("[it's]", s));
        assertEquals("it's, plain", PureRelationText.tdsCell("it's, plain", s));
    }

    /** A value's {@code toString()}: an Integer's digits; a Float column's cell the double it is declared, whatever
     *  kind the database held it in, written by legend-pure's Float print; a Number column's integer its digits and its
     *  fraction-kind value a Float; a Decimal at its scale; a Boolean; a date; a time. */
    @Test
    void aValueIsLegendPuresPrint() {
        assertEquals("7", PureValueText.cell(7L, col("i", "Integer")));
        assertEquals("2.0", PureValueText.cell(2.0d, col("f", "Float")));
        assertEquals("0.00001", PureValueText.cell(0.00001d, col("f", "Float")));
        assertEquals("100000000000000000000.0", PureValueText.cell(1e20d, col("f", "Float")));
        assertEquals("2.0", PureValueText.cell(2L, col("f", "Float")));
        assertEquals("81.18", PureValueText.cell(new BigDecimal("81.180"), col("f", "Float")));
        // a decimal the database held under a Float column is the double it is declared (the review of 2026-11-24)
        assertEquals("0.3333333333333333", PureValueText.cell(new BigDecimal("0.333333333333333333"),
                col("f", "Float")));
        assertEquals("3", PureValueText.cell(3L, col("n", "Number")));
        assertEquals("81.18", PureValueText.cell(new BigDecimal("81.180"), col("n", "Number")));
        assertEquals("1.50", PureValueText.cell(new BigDecimal("1.50"), col("d", "Decimal")));
        assertEquals("true", PureValueText.cell(true, col("b", "Boolean")));
        assertEquals("10:30:15", PureValueText.cell(new PureTimeLiteral.TimeWithSecond(10, 30, 15),
                col("t", "StrictTime")));
    }

    /** legend-pure's Float print (CompiledSupport: {@code 0.0} for either zero, else {@code DecimalFormat("0.0")} with
     *  340 fraction digits): the values where it differs from the shortest round-trip digits, as the review measured
     *  them on JDK 21 (2026-11-24). */
    @Test
    void aFloatIsLegendPuresDecimalFormat() {
        assertEquals("0.0", PureValueText.pureFloat(-0.0d));
        assertEquals("0.0", PureValueText.pureFloat(0.0d));
        assertEquals("-0.25", PureValueText.pureFloat(-0.25d));
        assertEquals("403018489792982720.0", PureValueText.pureFloat(4.030184897929827E17));
        assertEquals("99999999999999990000000.0", PureValueText.pureFloat(1e23));
        assertEquals("\u221E", PureValueText.pureFloat(Double.POSITIVE_INFINITY));
        assertEquals("-\u221E", PureValueText.pureFloat(Double.NEGATIVE_INFINITY));
        assertEquals("NaN", PureValueText.pureFloat(Double.NaN));
        assertEquals("0.3", PureValueText.pureFloat(0.3d));
        assertEquals("0.30000000000000004", PureValueText.pureFloat(0.1d + 0.2d));
    }

    /** A date: its day when it has no time; with one, its seconds, at least three subsecond digits (the finer ones
     *  without their trailing zeros) and {@code +0000} -- the edge's SQL composition's rule until B2b. */
    @Test
    void aDateIsItsDayOrItsTdsInstant() {
        assertEquals("2014-01-01", PureValueText.tdsDate(new PureDateLiteral.StrictDate(2014, 1, 1)));
        assertEquals("2014-01-01T10:20:30.000+0000",
                PureValueText.tdsDate(new PureDateLiteral.DateWithSecond(2014, 1, 1, 10, 20, 30)));
        assertEquals("2014-01-01T10:20:30.500+0000",
                PureValueText.tdsDate(new PureDateLiteral.DateWithSubsecond(2014, 1, 1, 10, 20, 30, "5")));
        assertEquals("2014-01-01T10:20:30.1234+0000",
                PureValueText.tdsDate(new PureDateLiteral.DateWithSubsecond(2014, 1, 1, 10, 20, 30, "1234")));
        assertEquals("2014-01-01T10:20:30.123456789+0000",
                PureValueText.tdsDate(new PureDateLiteral.DateWithSubsecond(2014, 1, 1, 10, 20, 30, "123456789")));
    }

    /** s.pure matches the value's runtime type: an enumeration's value is no String, so a brace in its name is not
     *  quoted (lite carries the value as its name, under the enumeration's column). */
    @Test
    void anEnumerationsValueIsNotQuotedAsAString() {
        assertEquals("{x}", PureRelationText.tdsCell("{x}", col("e", "my::Kind")));
        assertEquals("'{x}'", PureRelationText.tdsCell("{x}", col("s", "String")));
        assertEquals("'{x}'", PureRelationText.tdsCell("{x}", col("a", RowDecode.ANY)));
    }

    @Test
    void aCellWithNoRuleIsRefusedByName() {
        com.legend.error.NotImplementedException e = assertThrows(com.legend.error.NotImplementedException.class,
                () -> PureValueText.cell(List.of(1L), col("l", "Integer")));
        assertTrue(e.getMessage().contains("'l'") && e.getMessage().contains("not written at the edge"),
                e.getMessage());
        // a date toCSV's formats cannot write (legend-pure's format raises on a missing component)
        assertThrows(com.legend.error.NotImplementedException.class, () -> PureRelationText.csvCell(
                new PureDateLiteral.DateWithHour(2014, 1, 1, 10), col("d", "DateTime"), false));
    }

    /** A mixed column, whose cells a typed batch hands over without their kind's tag, is refused before any row. */
    @Test
    void aMixedColumnIsRefusedByName() {
        ExecutionPlan.Column mixed = new ExecutionPlan.Column("m", "Any", new ExecutionPlan.Multiplicity(0, 1),
                "TAGGED<INTEGER i, VARCHAR s>", null, null);
        assertThrows(com.legend.error.NotImplementedException.class,
                () -> PureRelationText.tds(List.of(mixed), List.of()));
    }

    // ---- helperFunctions.pure: toCSV, a TDS's csv ---------------------------------------------------------------

    /** helperFunctions.pure:208-250: the names and cells escaped by {@code escapeCSVString} (a {@code "} doubled), a
     *  date with a time {@code yyyy-MM-dd HH:mm:ss}, a day {@code yyyy-MM-dd}, an empty cell nothing, a newline after
     *  the last row. */
    @Test
    void theCsvOfHelperFunctionsPure() {
        List<ExecutionPlan.Column> cols = List.of(col("name", "String"), col("when", "DateTime"), col("day", "StrictDate"),
                col("n", "Integer"));
        List<List<Object>> rows = List.of(
                row("Smith, John", new PureDateLiteral.DateWithSubsecond(2014, 1, 1, 10, 20, 30, "5"),
                        new PureDateLiteral.StrictDate(2014, 1, 2), 3L),
                row("say \"hi\"", null, null, null));
        assertEquals("name,when,day,n\n\"Smith, John\",2014-01-01 10:20:30,2014-01-02,3\n\"say \"\"hi\"\"\",,,\n",
                PureRelationText.csv(cols, rows));
        assertEquals("name,when,day,n\n\"Smith, John\",2014-01-01 10:20:30,2014-01-02,3\n"
                + "\"say \"\"hi\"\"\",TDSNull,TDSNull,TDSNull\n", PureRelationText.csv(cols, rows, true));
    }

    @Test
    void noRowsCsvIsTheHeaderAndOneNewline() {
        assertEquals("a,b\n\n", PureRelationText.csv(List.of(col("a", "Integer"), col("b", "String")), List.of()));
    }

    /** a TDS's csv property (testEnumInRelation's golden): names joined ", ", an empty cell TDSNull, no last newline. */
    @Test
    void aTdsCsvPropertyIsTheCorpusGoldensForm() {
        List<ExecutionPlan.Column> cols = List.of(col("Name", "String"), col("Kind", "String"));
        assertEquals("Name, Kind\nA,X\nB,TDSNull", PureRelationText.tdsCsvProperty(cols,
                List.of(row("A", "X"), row("B", null))));
    }

    // ---- the program's literal replaces --------------------------------------------------------------------------

    /** The corpus's calendar cases print {@code toCSV(...)->replace('\n', ',')}: the replace applies to the printed
     *  text, every occurrence, in order. */
    @Test
    void literalReplacesApplyInOrder() {
        String csv = PureRelationText.csv(List.of(col("a", "Integer")), List.of(row(1L), row(2L)));
        assertEquals("a,1,2,", PrintTail.applied(csv, List.of(new PrintTail.Replace("\n", ","))));
        assertEquals("b-1", PrintTail.applied("a-1", List.of(new PrintTail.Replace("a", "b"))));
        assertEquals("cc", PrintTail.applied("aa", List.of(new PrintTail.Replace("a", "b"),
                new PrintTail.Replace("b", "c"))));
        // an empty target is Java's String.replace's, which Pure's replace is: before and after each character
        assertEquals("xax", PrintTail.applied("a", List.of(new PrintTail.Replace("", "x"))));
    }

    // ---- the writer: a run's receiver ----------------------------------------------------------------------------

    @Test
    void theWriterPrintsTheRowsAtTheEnd() throws Exception {
        List<String> got = new ArrayList<>();
        PureRelationTextWriter w = new PureRelationTextWriter(PureRelationTextWriter.Form.TO_CSV, got::add);
        w.answers(ExecutionPlan.TdsResult.Shape.RELATION);
        w.columns(List.of(col("a", "Integer")));
        w.end();
        // the text, and the text alone: a print's tail is the door's to apply (below)
        assertEquals(List.of("a\n\n"), got);
        assertThrows(IllegalStateException.class, w::end);
        assertThrows(IllegalStateException.class, () -> new PureRelationTextWriter(PureRelationTextWriter.Form.TO_CSV,
                got::add).end());
        assertThrows(IllegalStateException.class, () -> new PureRelationTextWriter(
                PureRelationTextWriter.Form.TO_STRING, got::add).answers(ExecutionPlan.TdsResult.Shape.VALUE));
    }

    /** A value's print (the edition program's step 8, 2026-11-26): the answer's one cell printed by legend-pure's
     *  rule, and handed to the receiver as the text it is. A print's tail is the DOOR's to apply to that text
     *  ({@link PrintedRelations.Side#answer}, {@link PrintedRelations.Lifted#answer}), never the writer's: a writer
     *  writes (the independent audit of slice 2, 2026-11-26; until then the writer applied it and handed back an
     *  Integer or a Boolean). */
    @Test
    void theWriterPrintsAValue_andTheDoorAppliesTheTail() throws Exception {
        List<String> got = new ArrayList<>();
        ExecutionPlan.Column f = new ExecutionPlan.Column("value", "Float", new ExecutionPlan.Multiplicity(1, 1),
                "DOUBLE", null, null);
        PureRelationTextWriter w = new PureRelationTextWriter(PureRelationTextWriter.Form.VALUE, got::add);
        w.answers(ExecutionPlan.TdsResult.Shape.VALUE);
        w.columns(List.of(f));
        w.batch(one(f, 1.3421e-8));
        w.end();
        assertEquals(List.of("0.000000013421"), got);
        com.legend.compiler.element.ModelContext none = com.legend.Compiler.compileModel("");
        assertEquals(14L, PrintedRelations.lift(com.legend.Compiler.query(none, "|(1.5 * 2.0)->toString()->length()"),
                true).answer(got.get(0)));
        assertEquals("0.000000013421", PrintedRelations.lift(com.legend.Compiler.query(none,
                "|(1.5 * 2.0)->toString()"), true).answer(got.get(0)));
        // a relation's rows do not answer a value's print, and no other type's value is printed
        assertThrows(IllegalStateException.class, () -> new PureRelationTextWriter(
                PureRelationTextWriter.Form.VALUE, got::add).answers(ExecutionPlan.TdsResult.Shape.RELATION));
        ExecutionPlan.Column i = new ExecutionPlan.Column("value", "Integer", new ExecutionPlan.Multiplicity(1, 1),
                "BIGINT", null, null);
        PureRelationTextWriter integer = new PureRelationTextWriter(PureRelationTextWriter.Form.VALUE, got::add);
        integer.columns(List.of(i));
        integer.batch(one(i, 1L));
        assertThrows(com.legend.error.NotImplementedException.class, integer::end);
    }

    /** A FLOAT'S LAST-STEP PRINT, VALUE BY VALUE (the independent audit of slice 2, 2026-11-26, its list): legend-pure's
     *  print ({@code CompiledSupport}'s {@code DecimalFormat("0.0")}, English, 340 fraction digits) of the double a
     *  database computed, whatever carrier it arrived in -- a point always, never an exponent at any magnitude, either
     *  zero {@code 0.0}, the value the double IS where a literal named one it cannot hold. Each text was run on the
     *  JDK the build pins (remotejdk_25: 25.0.4), which is what this print is held to: legend-pure's own print is the
     *  same code on whichever JDK it runs on (its build accepts 11, 17, 21 and 25), and a DecimalFormat's digits for
     *  some doubles are the JDK's ({@code 2.0E23} below). */
    @Test
    void aFloatsValuePrintIsLegendPuresOnTheBuildsJdk() {
        ExecutionPlan.Column f = new ExecutionPlan.Column("value", "Float", new ExecutionPlan.Multiplicity(1, 1),
                "DOUBLE", null, null);
        java.util.Map<Object, String> texts = new java.util.LinkedHashMap<>();
        texts.put(-0.0d, "0.0");
        texts.put(1.0E7d, "10000000.0");
        texts.put(1.0E-4d, "0.0001");
        texts.put(1.0E15d, "1000000000000000.0");
        texts.put(1.0E16d, "10000000000000000.0");
        texts.put(9007199254740993.0d, "9007199254740992.0");
        texts.put(1.7976931348623157E308d, "17976931348623157" + "0".repeat(292) + ".0");
        texts.put(4.9E-324d, "0." + "0".repeat(323) + "49");
        texts.put(1.0E23d, "99999999999999990000000.0");
        texts.put(2.0E23d, "199999999999999980000000.0");
        texts.put(0.1d + 0.2d, "0.30000000000000004");
        texts.put(Double.NaN, "NaN");
        texts.put(Double.POSITIVE_INFINITY, "\u221e");
        texts.put(Double.NEGATIVE_INFINITY, "-\u221e");
        // a Float a database hands back in another carrier: an integer (a store's INT column under a Float), a decimal
        texts.put(1000000L, "1000000.0");
        texts.put(7, "7.0");
        texts.put(java.math.BigInteger.valueOf(12), "12.0");
        texts.put(new BigDecimal("81.180"), "81.18");
        texts.put(new BigDecimal("0.3"), "0.3");
        for (var e : texts.entrySet()) {
            assertEquals(e.getValue(), PureValueText.value(e.getKey(), f), String.valueOf(e.getKey()));
        }
        assertThrows(com.legend.error.NotImplementedException.class, () -> PureValueText.value("1.5", f));
    }

    /** A DateTime the database computed, printed at a last step (the edition program's step 8, 2026-11-26): its
     *  seconds, the fraction without its trailing zeros (none: no point), and {@code +0000}, the text the edge's SQL
     *  composition wrote until it was deleted that day. A DateTime the database holds as a day is its midnight, as
     *  that SQL printed it; a month or a text is no DateTime's value: refused by name. */
    @Test
    void aComputedDateTimeIsItsSecondsAndTrimmedFraction() {
        ExecutionPlan.Column at = new ExecutionPlan.Column("value", "DateTime", new ExecutionPlan.Multiplicity(1, 1),
                "TIMESTAMP", null, null);
        java.util.Map<String, String> texts = new java.util.LinkedHashMap<>();
        texts.put("2014-01-02T00:00:00.000000000", "2014-01-02T00:00:00+0000");
        texts.put("2014-01-01T11:20:30.250000000", "2014-01-01T11:20:30.25+0000");
        texts.put("2014-01-01T10:20:32.123456000", "2014-01-01T10:20:32.123456+0000");
        texts.put("2014-01-01T10:20:32.000000001", "2014-01-01T10:20:32.000000001+0000");
        texts.put("2014-01-01T10:20:30", "2014-01-01T10:20:30+0000");
        texts.put("2014-01-01T10:20", "2014-01-01T10:20:00+0000");
        // a year below 1000 is written as the number it is, unpadded: legend-pure's own print
        // (legend-pure-m4 DateFormat.append, line 240: safeAppendable.append(date.getYear()), an int; the outside
        // audit of slices 3 and 4, S3)
        texts.put("0999-03-04T10:20:30.500000000", "999-03-04T10:20:30.5+0000");
        for (var e : texts.entrySet()) {
            assertEquals(e.getValue(), PureValueText.value(com.legend.values.PureDateLiteral.parse(e.getKey()), at),
                    e.getKey());
        }
        assertEquals("2014-01-01T00:00:00+0000",
                PureValueText.value(com.legend.values.PureDateLiteral.parse("2014-01-01"), at));
        assertThrows(com.legend.error.NotImplementedException.class,
                () -> PureValueText.value(com.legend.values.PureDateLiteral.parse("2014-01"), at));
        assertThrows(com.legend.error.NotImplementedException.class, () -> PureValueText.value("2014-01-01", at));
    }

    /** A batch of one row of one cell. */
    private static com.legend.result.Batch one(ExecutionPlan.Column column, Object cell) {
        com.legend.result.Batch.Builder b = com.legend.result.Batch.builder(List.of(column), 1);
        b.add(List.of(cell));
        return b.build();
    }
}
