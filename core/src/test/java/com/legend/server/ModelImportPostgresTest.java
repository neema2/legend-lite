// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testing.EmbeddedPostgres;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@link ModelImportTest#constraintsKeepWhatTheSchemaKeeps} on a REAL Postgres (docs/MODEL_IMPORT_STUDY_2026_10_20.md
 * §4.4): the constraints an imported schema's validations and logic make, each run through the Postgres dialect as a
 * query's filter (its pattern RE2's, as the platform writes them, in Postgres's own flavour), reject the same rows as on
 * DuckDB, and keep the rows JSON Schema keeps. Its own target ({@code //core:model_import_postgres_test}), as
 * {@code SqlIslandPostgresTest}: it needs the embedded Postgres in its runfiles.
 */
@DisplayName("Model import on Postgres -- the constraints keep what the schema keeps")
class ModelImportPostgresTest {

    @Test
    @DisplayName("each constraint, through the Postgres dialect, rejects the rows it rejects on DuckDB")
    void constraintsKeepWhatTheSchemaKeeps() throws Exception {
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl("postgres"))) {
            // a connection with auth: Test logs in as the operating-system account (SqlIslandPostgresTest)
            String account = "\"" + System.getProperty("user.name").replace("\"", "\"\"") + "\"";
            try (PreparedStatement exists = admin.prepareStatement("SELECT 1 FROM pg_roles WHERE rolname = ?")) {
                exists.setString(1, System.getProperty("user.name"));
                try (ResultSet r = exists.executeQuery()) {
                    if (!r.next()) {
                        try (Statement create = admin.createStatement()) {
                            create.execute("CREATE ROLE " + account + " LOGIN");
                        }
                    }
                }
            }
            try (Statement s = admin.createStatement()) {
                s.execute("DROP TABLE IF EXISTS PAYMENTS");
            }
            Map<String, List<String>> rejected = ModelImportTest.rejectedOn(String.format(Locale.ROOT,
                    "type: Postgres; specification: Static { host: '127.0.0.1'; port: %d; name: 'postgres'; }; "
                            + "auth: Test;", pg.port()), admin);
            assertEquals(ModelImportTest.REJECTED, rejected, "each constraint, the rows it rejects");
            assertEquals(List.of("1", "3", "14"), ModelImportTest.valid(rejected), "the rows JSON Schema keeps");
        }
    }
}
