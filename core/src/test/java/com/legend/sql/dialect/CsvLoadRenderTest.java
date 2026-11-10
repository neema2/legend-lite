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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A CSV file's load as each dialect spells it ({@link SqlDialect#csvLoad}; docs/DATA_IN_THE_PLAN_2026_11_09.md L1,
 * 2026-11-09): no statement names a file or holds a row. DuckDB reads the runner's copy through its statement's one
 * bound {@code ?}; H2's {@code CSVREAD} takes no bound path, so the runner streams the cells through one row's insert;
 * a dialect whose database takes neither refuses it by name. The loads themselves run in {@code exec.DataLoadsTest}.
 */
class CsvLoadRenderTest {

    private static final SqlDml.LoadCsv LOAD = new SqlDml.LoadCsv(null, "people", NameQuoting.BARE,
            List.of("FIRSTNAME", "LASTNAME", "AGE"), Set.of());

    @Test
    void duckDbReadsTheRunnersCopyThroughABoundPath() {
        SqlDialect.CsvLoad load = new DuckDb().csvLoad(LOAD);
        assertTrue(load.readsFile());
        assertEquals("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) SELECT * FROM read_csv(?, header = true,"
                + " delim = ',', quote = '\"', escape = '\"', all_varchar = true)", load.sql());
    }

    @Test
    void h2TakesTheCellsTheRunnerStreams() {
        SqlDialect.CsvLoad load = new H2().csvLoad(LOAD);
        assertFalse(load.readsFile());
        assertEquals("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) VALUES (?, ?, ?)", load.sql());
    }

    /** The cells the runner streams to H2 are the cells H2's own {@code CSVREAD} read until 2026-11-09: the split
     *  rule ({@code base.CsvCells}) measured against the reader itself. (Not upstream's reader: register row S172; the
     *  design's L2 moves the rule to the declared format's default and this test to that reader.) */
    @Test
    void theStreamedCellsAreTheCellsCsvreadReads(@TempDir Path dir) throws Exception {
        String text = "A,B,C\r\n \"x, \"\"y\"\"\" , z ,\n\nlast\r\n\"\",,  \n  padded  ,\"q\"\t,9,extra\n";
        Path file = Files.writeString(dir.resolve("cells.csv"), text);
        List<List<String>> read = new ArrayList<>();
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:csv_cells"); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM CSVREAD('" + file + "', NULL, 'charset=UTF-8')")) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= width; i++) {
                    row.add(rs.getString(i));
                }
                read.add(row);
            }
        }
        List<List<String>> rows = com.legend.base.CsvCells.rows(text);
        assertEquals(read, com.legend.base.CsvCells.body(rows, rows.get(0).size()));
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
