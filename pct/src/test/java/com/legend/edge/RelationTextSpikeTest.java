// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.values.PureDateLiteral;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SPIKE (H11, docs/records/compat-homework/H11-relation-text-spike.md): the Java printer of legend-pure's relation
 * texts over the cases PCT does not reach -- the PR #118 audit's EDGE-1, EDGE-2 and EDGE-3, and the carriers PCT's
 * 431 texts never held. Each expected text is upstream's rule read at the pin, cited beside it.
 */
class RelationTextSpikeTest {

    private static List<RelationText.Col> cols(String... nameThenType) {
        RelationText.Col[] out = new RelationText.Col[nameThenType.length / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = new RelationText.Col(nameThenType[2 * i], nameThenType[2 * i + 1]);
        }
        return List.of(out);
    }

    private static List<List<Object>> rows(Object[]... rows) {
        return Arrays.stream(rows).map(Arrays::asList).toList();
    }

    private static Object[] row(Object... cells) {
        return cells;
    }

    // EDGE-1: a Float cell at the audit's five values. legend-pure: FloatCoreInstance.canonicalizeBigDecimal(
    // BigDecimal.valueOf(d)).toPlainString() -- a point always, never an exponent.
    @Test
    void edge1_aFloatCell_inToStringAndToCsv() {
        double[] values = {0.00001, 0.0005, 25000000.0, 10000000000000000.0, 100000000000000000000.0};
        String[] texts = {"0.00001", "0.0005", "25000000.0", "10000000000000000.0", "100000000000000000000.0"};
        for (int i = 0; i < values.length; i++) {
            assertEquals("#TDS\n   x\n   " + texts[i] + "\n#",
                    RelationText.tds(cols("x", "Float"), rows(row(values[i]))));
            assertEquals("x\n" + texts[i] + "\n", RelationText.csv(cols("x", "Float"), rows(row(values[i])), false));
        }
    }

    // EDGE-2: toCSV's DateTime. legend-engine: formatDateTime (helperFunctions.pure:238-243), hasHour ->
    // format('%t{yyyy-MM-dd HH:mm:ss}') (dateExtension.pure:384-387), else '%t{yyyy-MM-dd}'.
    @Test
    void edge2_toCsvsDateTime_isUpstreamsSimpleDateTimeFormat() {
        assertEquals("id,at\n1,2024-01-02 10:30:00\n", RelationText.csv(cols("id", "Integer", "at", "DateTime"),
                rows(row(1L, PureDateLiteral.parse("2024-01-02T10:30:00.123"))), false));
        assertEquals("d\n2024-01-02\n", RelationText.csv(cols("d", "StrictDate"),
                rows(row(PureDateLiteral.parse("2024-01-02"))), false));
    }

    // EDGE-3: format('%r') of a String holding a newline. legend-pure: toRepresentation.pure:43 escapes \, ' and the
    // newline (as the two characters \n).
    @Test
    void edge3_representationEscapesANewline() {
        assertEquals("'a\\nb'", RelationText.representation("a\nb"));
        assertEquals("'it\\'s \\\\ here'", RelationText.representation("it's \\ here"));
    }

    // a date cell of a relation's toString: TestTDS.java:209 re-reads every date at three subsecond digits, truncated
    @Test
    void aDateCellOfToString_isThreeSubsecondDigits() {
        assertEquals("#TDS\n   at\n   1985-01-12T06:30:00.000+0000\n   2024-01-02T10:30:00.123+0000\n   null\n#",
                RelationText.tds(cols("at", "DateTime"), rows(row(PureDateLiteral.parse("1985-01-12T06:30:00")),
                        row(PureDateLiteral.parse("2024-01-02T10:30:00.123456789")), row((Object) null))));
    }

    // the carriers PCT's texts never held
    @Test
    void theNumbersPctNeverHeld() {
        // a whole number under a Float column is a Float (TestTDS.java:246-253)
        assertEquals("#TDS\n   f\n   3.0\n#", RelationText.tds(cols("f", "Float"), rows(row(3L))));
        // a decimal the database computed under a Float or a Number column prints as the Float it is declared
        assertEquals("#TDS\n   f,n\n   81.18,3.0\n#", RelationText.tds(cols("f", "Float", "n", "Number"),
                rows(row(new BigDecimal("81.180"), new BigDecimal("3.000")))));
        // a Decimal at its own scale, no D (SimpleDecimalCoreInstance.getName: toPlainString)
        assertEquals("#TDS\n   d\n   21.50\n#", RelationText.tds(cols("d", "Decimal"), rows(row(new BigDecimal("21.50")))));
        // a whole number under a Number column keeps its digits (as the SQL builder printed it)
        assertEquals("#TDS\n   n\n   3\n#", RelationText.tds(cols("n", "Number"), rows(row(3L))));
    }

    // the header and the cell's quoting (toString.pure:27-30, s.pure:23-38)
    @Test
    void namesAndCellsAreQuotedAsUpstreamQuotesThem() {
        assertEquals("#TDS\n   a,'b c','d'\n   x,'{\"k\":1}','it\\'s [1]'\n   null,'null',null\n#",
                RelationText.tds(cols("a", "String", " b c ", com.legend.result.RowDecode.VARIANT, "'d'", "String"),
                        rows(row("x", "{\"k\":1}", "it's [1]"), row(null, null, null))));
        // no rows: the rows segment is empty
        assertEquals("#TDS\n   a\n\n#", RelationText.tds(cols("a", "Integer"), List.of()));
    }

    // toCSV's escape and its empty cell (helperFunctions.pure:229-250), and a TDS's csv property
    @Test
    void csvEscapesAndSpellsAnEmptyCell() {
        List<RelationText.Col> c = cols("n,ame", "String", "v", "Integer");
        List<List<Object>> r = rows(row("a\"b", 1L), row("x\ny", null));
        assertEquals("\"n,ame\",v\n\"a\"\"b\",1\n\"x\ny\",\n", RelationText.csv(c, r, false));
        assertEquals("\"n,ame\",v\n\"a\"\"b\",1\n\"x\ny\",TDSNull\n", RelationText.csv(c, r, true));
        assertEquals("v\n\n", RelationText.csv(cols("v", "Integer"), List.of(), false));
        assertEquals("name, type\nAlice,CONTRACT\nCurtis,TDSNull", RelationText.tdsCsvProperty(
                cols("name", "String", "type", "my::Enum"), rows(row("Alice", "CONTRACT"), row("Curtis", null))));
    }
}
