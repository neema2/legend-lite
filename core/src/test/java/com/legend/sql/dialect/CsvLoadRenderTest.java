// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;
import com.legend.sql.NameQuoting;
import com.legend.sql.SqlDml;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A CSV file's load as each dialect spells it ({@link SqlDialect#csvLoad}; docs/DATA_IN_THE_PLAN_2026_11_09.md L1 and
 * L2, 2026-11-09): no statement names a file or holds a row. Every database that takes a file reads it itself, by its
 * declared format (decision 6; the user's ruling of 2026-11-09: an option its reader cannot read is refused); a dialect
 * whose database takes none refuses it by name. The format's cells as upstream reads them are {@code base.CsvCells}'s,
 * the referee's copy. The loads themselves run in
 * {@code exec.DataLoadsTest}.
 */
class CsvLoadRenderTest {

    private static final SqlDml.LoadCsv LOAD = new SqlDml.LoadCsv(null, "people", NameQuoting.BARE,
            List.of("FIRSTNAME", "LASTNAME", "AGE"), Set.of(), ',', '"');

    /** The database reads the file (the user's ruling, 2026-11-09: an option its reader cannot read is refused, never
     *  streamed by Java): the statements bind the staged file's name to their one {@code ?} and name no file. */
    @Test
    void duckDbAndH2ReadTheFileThemselves() {
        assertEquals(List.of("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) SELECT * FROM read_csv(?, header = true,"
                + " delim = ',', quote = '\"', escape = '\"', auto_detect = false, strict_mode = true, nullstr = '',"
                + " columns = {'c0': 'VARCHAR', 'c1': 'VARCHAR', 'c2': 'VARCHAR'})"),
                new DuckDb().csvLoad(LOAD).statements());
        List<String> h2 = new H2().csvLoad(LOAD).statements();
        assertEquals("SET @legend_csv = ?", h2.get(0));
        assertEquals("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) SELECT NULLIF(C0, ''), NULLIF(C1, ''), NULLIF(C2, '')"
                + " FROM (SELECT * FROM CSVREAD(@legend_csv, 'C0,C1,C2,C3', 'charset=UTF-8 fieldSeparator=,"
                + " fieldDelimiter=\" escape=\" preserveWhitespace=true') OFFSET 1 ROW) AS x WHERE CASE"
                + " WHEN \"C3\" IS NOT NULL THEN SIGNAL('45000', CHAR(31) || ('a record of the CSV file has more cells"
                + " than its table''s 3 column(s)') || CHAR(31)) WHEN \"C1\" IS NULL THEN SIGNAL('45000', CHAR(31) || ('a"
                + " record of the CSV file has fewer cells than its table''s 3 column(s), or the file ends inside a quoted"
                + " cell (EOF reached before encapsulated token finished)') || CHAR(31)) ELSE TRUE END", h2.get(1));
    }

    /** H2's {@code CSVREAD} options are split on white space: a white-space delimiter or quote is refused by name. */
    @Test
    void aWhiteSpaceDelimiterIsRefusedOnH2() {
        for (char c : new char[] {' ', '\t'}) {
            SqlDml.LoadCsv load = new SqlDml.LoadCsv(null, "people", NameQuoting.BARE, LOAD.columns(), Set.of(), c, '"');
            DialectCapability e = assertThrows(DialectCapability.class, () -> new H2().csvLoad(load));
            assertTrue(e.getMessage().contains("white space"), e.getMessage());
        }
    }

    /** {@code cells}, its nulls kept. */
    private static List<String> row(String... cells) {
        return java.util.Arrays.asList(cells);
    }

    /**
     * The declared format's default rule ({@code base.CsvCells}) is upstream's: each case's records are what
     * legend-pure's {@code loadCsvToDbTable} reads (Commons CSV 1.5, {@code CSVFormat.EXCEL}, an empty cell NULL), measured
     * 2026-11-09 with that library (docs/data-in-the-plan-homework/probes/csv_format_probe.java.txt: these cases, and
     * 2,000,000 random texts with no difference).
     */
    @Test
    void theFormatsCellsAreUpstreams() {
        java.util.Map<String, List<List<String>>> cases = new java.util.LinkedHashMap<>();
        cases.put("A,B,C\n  Joe  ,\"\",\n", List.of(row("A", "B", "C"), row("  Joe  ", null, null)));
        cases.put("A,B,C\r\nx,\"a \"\"q\"\" b\",z\r\n", List.of(row("A", "B", "C"), row("x", "a \"q\" b", "z")));
        cases.put("A,B,C\n\"x\" ,y,z", List.of(row("A", "B", "C"), row("x", "y", "z")));
        cases.put("A,B\n \"x\",y", List.of(row("A", "B"), row(" \"x\"", "y")));
        cases.put("A,B\na\"b,c", List.of(row("A", "B"), row("a\"b", "c")));
        cases.put("A\nx\n\ny\n", List.of(row("A"), row("x"), row((String) null), row("y")));
        cases.put("A,B\r1,2\r", List.of(row("A", "B"), row("1", "2")));
        cases.put("A,B\n\"l1\nl2\",z", List.of(row("A", "B"), row("l1\nl2", "z")));
        cases.put("A,B\nx,", List.of(row("A", "B"), row("x", null)));
        cases.forEach((text, rows) -> assertEquals(rows, com.legend.base.CsvCells.rows(text), text));
        // upstream fails on these: a character between a closing quote and the delimiter, a text ending in a quote
        for (String text : List.of("A,B\n\"x\"q,y", "A,B\n\"x,y\n")) {
            assertThrows(com.legend.base.CsvCells.Malformed.class, () -> com.legend.base.CsvCells.rows(text), text);
        }
        // a data record of another width than the table's is refused, never padded or cut (upstream binds too few values,
        // or fails on the extra cell); an empty line is a record of one cell, a NULL row in a one-column table
        for (String text : List.of("A,B\nx", "A,B\nx,y,z", "A,B\nx,y\n\nz,w")) {
            assertThrows(com.legend.base.CsvCells.Malformed.class,
                    () -> com.legend.base.CsvCells.body(com.legend.base.CsvCells.rows(text), 2), text);
        }
    }

    @Test
    void aDatabaseThatTakesNoFileRefusesByName() {
        for (SqlDialect d : List.of(new SQLite(), new Postgres(), new MongoDb())) {
            DialectCapability e = assertThrows(DialectCapability.class, () -> d.csvLoad(LOAD),
                    d.getClass().getSimpleName());
            assertTrue(e.getMessage().contains("CSV file's load"), e.getMessage());
        }
    }
}
