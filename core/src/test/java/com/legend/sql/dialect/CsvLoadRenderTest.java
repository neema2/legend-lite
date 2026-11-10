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
 * L2, 2026-11-09): no statement names a file or holds a row. DuckDB reads a file itself, by its declared format (the
 * user's ruling of 2026-11-09); every other database takes the runner's cells by upstream's rule (the user's ruling of
 * 2026-11-10); MongoDB refuses one by name. The format's cells as upstream reads them are {@code base.CsvCells}'s,
 * the referee's copy. The loads themselves run in
 * {@code exec.DataLoadsTest}.
 */
class CsvLoadRenderTest {

    private static final SqlDml.LoadCsv LOAD = new SqlDml.LoadCsv(null, "people", NameQuoting.BARE,
            List.of("FIRSTNAME", "LASTNAME", "AGE"), Set.of(),
            List.of(new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", 200),
            new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", 200),
            new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.INTEGER)), ',', '"');

    /** DuckDB reads the file itself (the user's ruling, 2026-11-09): its statement binds the staged file's name to its
     *  one {@code ?} and names no file. Every other database takes the runner's cells by upstream's rule into one row's
     *  insert (the user's ruling, 2026-11-10, "Okay I guess that can be used for databases that have no csv read
     *  then"). */
    @Test
    void duckDbReadsTheFileAndEveryOtherDatabaseTakesTheCells() {
        assertEquals(new SqlDialect.CsvLoad.Reads(List.of("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) SELECT * FROM"
                + " read_csv(?, header = true, delim = ',', quote = '\"', escape = '\"', auto_detect = false,"
                + " strict_mode = true, nullstr = '', columns = {'c0': 'VARCHAR', 'c1': 'VARCHAR', 'c2': 'VARCHAR'})")),
                new DuckDb().csvLoad(LOAD));
        for (SqlDialect d : List.of(new H2(), new SQLite())) {
            assertEquals(new SqlDialect.CsvLoad.Streamed("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) VALUES (?, ?, ?)"),
                    d.csvLoad(LOAD), d.getClass().getSimpleName());
        }
        // Postgres folds an unquoted name to lower case, and casts each bound text cell to its column's declared type
        assertEquals(new SqlDialect.CsvLoad.Streamed("INSERT INTO people (firstname, lastname, age) VALUES (CAST(? AS"
                + " VARCHAR(200)), CAST(? AS VARCHAR(200)), CAST(? AS INTEGER))"), new Postgres().csvLoad(LOAD));
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
        for (SqlDialect d : List.of(new MongoDb())) {
            DialectCapability e = assertThrows(DialectCapability.class, () -> d.csvLoad(LOAD),
                    d.getClass().getSimpleName());
            assertTrue(e.getMessage().contains("CSV file's load"), e.getMessage());
        }
    }
}
