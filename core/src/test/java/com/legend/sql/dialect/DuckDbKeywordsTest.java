// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * DuckDB's own keyword table against the renderer's quoting list: every keyword DuckDB will not take as a plain table
 * or column name ({@code reserved} and {@code type_function} in {@code duckdb_keywords()}) is quoted by
 * {@link Lexicon#DUCKDB}. A table named {@code aT} rendered unquoted failed to parse (found 2026-10-05 when Bazel
 * workplan P3-17 made ResolveUnionV4ProbeTest run its SQL); a DuckDB bump that reserves a new word fails here.
 */
class DuckDbKeywordsTest {

    @Test
    void everyKeywordDuckDbRefusesAsANameIsQuoted() throws SQLException {
        TreeSet<String> missing = new TreeSet<>();
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                ResultSet rs = c.createStatement().executeQuery("SELECT keyword_name FROM duckdb_keywords()"
                        + " WHERE keyword_category IN ('reserved', 'type_function')")) {
            while (rs.next()) {
                String word = rs.getString(1).toLowerCase(java.util.Locale.ROOT);
                if (!Lexicon.DUCKDB.reservedWords().contains(word)) {
                    missing.add(word);
                }
            }
        }
        assertEquals(new TreeSet<String>(), missing, "keywords DuckDB refuses as names that Lexicon.DUCKDB renders bare");
    }

    @Test
    void aKeywordNamedTableRendersQuoted() {
        assertEquals("\"aT\"", new DuckDb().physicalName("aT"));
        assertEquals("plain", new DuckDb().physicalName("plain"));
    }
}
