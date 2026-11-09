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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A CSV file's load ({@link SqlDml.LoadCsv}, {@code loadCsvToDbTable}; the user's ruling, 2026-11-06): the statement
 * names the file and the DATABASE reads and parses it -- DuckDB's {@code read_csv}, H2's {@code CSVREAD}, as
 * legend-engine's H2 loads one -- its header dropped, its cells cast into the table's columns positionally. A dialect
 * whose database reads no file refuses it by name.
 */
class CsvLoadRenderTest {

    private static SqlDml.LoadCsv load(Path file) {
        return new SqlDml.LoadCsv(null, "people", NameQuoting.BARE, List.of("FIRSTNAME", "LASTNAME", "AGE"), Set.of(),
                file.toString());
    }

    @Test
    void theEmbeddedDatabasesReadTheFileThemselves(@TempDir Path dir) throws Exception {
        // the corpus's file as it is: a header, two rows, no newline after the last
        Path file = Files.writeString(dir.resolve("employees.csv"), "Firstname,Lastname,Age\nJoe,Bloggs,21\nJill,Smith,31");
        for (var db : List.<Map.Entry<SqlDialect, String>>of(Map.entry(new H2(), "jdbc:h2:mem:csv_load"),
                Map.entry(new DuckDb(), "jdbc:duckdb:"))) {
            SqlDialect d = db.getKey();
            String name = d.getClass().getSimpleName();
            String sql = d.render(load(file));
            assertTrue(sql.startsWith("INSERT INTO people (FIRSTNAME, LASTNAME, AGE) SELECT * FROM "), name + ": " + sql);
            assertTrue(sql.contains(file.toString()), name + " names the file, never its rows: " + sql);
            try (Connection c = DriverManager.getConnection(db.getValue()); Statement st = c.createStatement()) {
                st.execute("CREATE TABLE people (FIRSTNAME VARCHAR(200), LASTNAME VARCHAR(200), AGE INT)");
                st.execute(sql);
                try (ResultSet rs = st.executeQuery("SELECT FIRSTNAME, LASTNAME, AGE FROM people ORDER BY AGE")) {
                    assertTrue(rs.next(), name);
                    assertEquals("Joe", rs.getString(1), name);
                    assertEquals("Bloggs", rs.getString(2), name);
                    assertEquals(21, rs.getInt(3), name);
                    assertTrue(rs.next(), name);
                    assertEquals("Jill", rs.getString(1), name);
                    assertEquals(31, rs.getInt(3), name);
                    assertFalse(rs.next(), name);
                }
            }
        }
    }

    @Test
    void aDatabaseThatReadsNoFileRefusesByName(@TempDir Path dir) {
        for (SqlDialect d : List.of(new SQLite(), new Postgres(), new MongoDb())) {
            DialectCapability e = assertThrows(DialectCapability.class, () -> d.render(load(dir.resolve("x.csv"))),
                    d.getClass().getSimpleName());
            assertTrue(e.getMessage().contains("CSV file's load"), e.getMessage());
        }
    }
}
