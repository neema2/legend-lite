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
 * L2, 2026-11-09; docs/DATA_DELIVERY_DESIGN_2026_11_10.md A3): no statement names a file or holds a row. DuckDB and H2
 * read a file themselves, by its declared format; every other database refuses one by name. The format's cells as
 * upstream reads them are {@code base.CsvCells}'s, the referee's copy alone (census M15, until B2b). The loads themselves run in
 * {@code exec.DataLoadsTest}.
 */
class CsvLoadRenderTest {

    private static final SqlDml.LoadCsv LOAD = new SqlDml.LoadCsv(null, "people", NameQuoting.BARE,
            List.of("FIRSTNAME", "LASTNAME", "AGE"), Set.of(),
            List.of(new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", 200),
            new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", 200),
            new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.INTEGER)), ',', '"', 500);

    /** DuckDB and H2 read the file themselves (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §1.3, A3): each statement binds
     *  the runner's copy's path to its one {@code ?} and names no file. DuckDB's strict reader refuses a record of
     *  another width itself; H2 reads one column more than the table, its record check refusing a wide record in the
     *  database. */
    @Test
    void duckDbAndH2ReadTheFileThemselves() {
        assertEquals(new SqlDialect.CsvLoad(List.of("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) SELECT * FROM"
                + " read_csv(?, header = true, delim = ',', quote = '\"', escape = '\"', auto_detect = false,"
                + " strict_mode = true, nullstr = '', max_line_size = 524288000, columns = {'c0': 'VARCHAR', 'c1':"
                + " 'VARCHAR', 'c2': 'VARCHAR'})")), new DuckDb().csvLoad(LOAD));
        SqlDialect.CsvLoad h2 = new H2().csvLoad(LOAD);
        assertEquals("SET @legend_csv = ?", h2.statements().get(0));
        String check = new H2().csvRecordCheck(3);
        assertTrue(check.contains("C3") && check.contains("IS NOT NULL") && !check.contains("C2"), check);
        assertEquals("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) SELECT NULLIF(C0, ''), NULLIF(C1, ''), NULLIF(C2,"
                + " '') FROM (SELECT * FROM CSVREAD(@legend_csv, 'C0,C1,C2,C3', 'charset=UTF-8 fieldSeparator=,"
                + " fieldDelimiter=\" escape=\" preserveWhitespace=true') OFFSET 1 ROW) AS x WHERE " + check,
                h2.statements().get(1));
        assertEquals("SET @legend_csv = NULL", h2.statements().get(2));
    }

    /** Every other database reads no file of the runner's and refuses a load by name (option 1, the user's go on the
     *  design, 2026-11-10): the runner splits no file. */
    @Test
    void everyOtherDatabaseRefusesAFileByName() {
        List<String> refusing = new java.util.ArrayList<>();
        for (com.legend.model.ConnectionDefinition.DatabaseType type : com.legend.database.Databases.implemented()) {
            SqlDialect d = com.legend.database.Databases.dialect(type);
            if (d instanceof DuckDb || d.getClass() == H2.class) {
                continue;
            }
            DialectCapability e = assertThrows(DialectCapability.class, () -> d.csvLoad(LOAD), type.name());
            assertTrue(e.getMessage().contains("CSV file's load"), e.getMessage());
            refusing.add(type.name());
        }
        assertTrue(refusing.contains("SQLite") && refusing.contains("Postgres"), String.valueOf(refusing));
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
}
