// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;


import com.legend.sql.SqlDdl;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A schema's DDL is the IR's ({@link SqlDdl.CreateSchema}, {@link SqlDdl.DropSchema}), rendered by a dialect (the
 * dialect program's Phase 3, 2026-10-16). Until then {@code dropAndCreateSchemaInDb} and the toDDL schema statements
 * wrote the text themselves.
 */
class SchemaDdlTextTest {

    /** The executed copy is the session's: valid on H2 and DuckDB, and none to run on SQLite, which has no schemas. */
    @Test
    void eachSessionsDialectSpellsItsOwn() throws Exception {
        SqlDdl create = new SqlDdl.CreateSchema("s1", false);
        for (var db : java.util.List.of(java.util.Map.entry(new H2(), "jdbc:h2:mem:schemaddl"),
                java.util.Map.entry(new DuckDb(), "jdbc:duckdb:"))) {
            try (Connection c = DriverManager.getConnection(db.getValue()); Statement st = c.createStatement()) {
                String sql = db.getKey().render(create);
                st.execute(sql);
                st.execute(sql);
            }
        }
        assertEquals("SELECT 1;", new SQLite().render(create));
    }
}
