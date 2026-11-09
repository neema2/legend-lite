// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;
import com.legend.sql.NameQuoting;
import com.legend.sql.SqlDml;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A CSV file's load as each dialect spells it ({@link SqlDialect#csvLoad}; docs/DATA_IN_THE_PLAN_2026_11_09.md L1 and
 * L2, 2026-11-09): no statement names a file or holds a row. Every database that takes a file takes the cells the
 * runner streams through one row's insert, split by the file's declared format, whose default is upstream's rule
 * (decision 6); a dialect whose database takes none refuses it by name. The loads themselves run in
 * {@code exec.DataLoadsTest}.
 */
class CsvLoadRenderTest {

    private static final SqlDml.LoadCsv LOAD = new SqlDml.LoadCsv(null, "people", NameQuoting.BARE,
            List.of("FIRSTNAME", "LASTNAME", "AGE"), Set.of());

    @Test
    void duckDbAndH2TakeTheCellsTheRunnerStreams() {
        for (SqlDialect d : List.of(new DuckDb(), new H2())) {
            assertEquals("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) VALUES (?, ?, ?)", d.csvLoad(LOAD).sql(),
                    d.getClass().getSimpleName());
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

    /** Why DuckDB takes the streamed cells and not its own reader (2026-11-09, L2): configured as strictly as it
     *  goes, {@code read_csv} still reads a cell whose quote follows a space as quoted (upstream: its text) and skips an
     *  empty line in a wider table (upstream: a record of one cell, refused). Should DuckDB's reader come to read the
     *  format exactly, this fails, and its reader may be weighed again. */
    @Test
    void duckDbsOwnReaderReadsTheFormatOtherwise(@TempDir Path dir) throws Exception {
        Path file = Files.writeString(dir.resolve("cells.csv"), "A,B\n \"x\",y\n\nz,w\n");
        List<List<String>> read = new ArrayList<>();
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                java.sql.PreparedStatement st = c.prepareStatement("SELECT * FROM read_csv(?, header = true,"
                        + " auto_detect = false, delim = ',', quote = '\"', escape = '\"',"
                        + " columns = {'c1': 'VARCHAR', 'c2': 'VARCHAR'}, strict_mode = true, null_padding = false)")) {
            st.setString(1, file.toString());
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    read.add(row(rs.getString(1), rs.getString(2)));
                }
            }
        }
        assertEquals(List.of(row("x", "y"), row("z", "w")), read);
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
