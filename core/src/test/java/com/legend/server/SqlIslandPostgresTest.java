// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.testing.Lean;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.SqlIslandCases;
import com.legend.testing.EmbeddedPostgres;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The {@code #SQL{...}#} cases on a REAL Postgres (docs/LEGEND_SQL_STUDY_2026_10_11.md §3): Postgres is the spec, so
 * each case's recorded answer must be Postgres's own (run as plain SQL), and the island must give the same rows when
 * legend-lite lowers it through the Postgres dialect. Its own target ({@code //core:sql_island_postgres_test}), as
 * {@code PostgresArmTest}: it needs the embedded Postgres in its runfiles.
 */
class SqlIslandPostgresTest {

    private static String model;

    @BeforeAll
    static void seed() throws SQLException {
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl("postgres"))) {
            // a connection with auth: Test logs in as the operating-system account (PostgresArmTest): it needs a
            // login role, and it reads the table
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
                s.execute("DROP TABLE IF EXISTS EMP");
                s.execute(SqlIslandCases.table(TestBackends.of(DatabaseType.Postgres)));
                s.execute("INSERT INTO EMP VALUES " + SqlIslandCases.ROWS);
                s.execute("GRANT SELECT ON EMP TO " + account);
            }
        }
        model = SqlIslandCases.model(String.format(Locale.ROOT, "type: Postgres;\n specification: Static {"
                + " host: '127.0.0.1'; port: %d; name: 'postgres'; };\n auth: Test;", pg.port()));
    }

    static Stream<Arguments> cases() {
        return SqlIslandCases.CASES.stream().map(c -> Arguments.of(c.sql(), c));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void theRecordedAnswerIsPostgres(String sql, SqlIslandCases.Case c) throws SQLException {
        try (Connection pg = DriverManager.getConnection(EmbeddedPostgres.shared().jdbcUrl("postgres"));
                Statement s = pg.createStatement();
                ResultSet r = s.executeQuery(c.plain())) {
            int n = r.getMetaData().getColumnCount();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= n; i++) {
                columns.add(r.getMetaData().getColumnLabel(i));
            }
            List<List<String>> rows = new ArrayList<>();
            while (r.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= n; i++) {
                    row.add(SqlIslandCases.cell(r.getObject(i)));
                }
                rows.add(row);
            }
            assertEquals(c.columns(), columns, sql);
            assertRows(c, rows);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void theIslandThroughThePostgresDialectAgrees(String sql, SqlIslandCases.Case c) throws SQLException {
        try (Connection pg = DriverManager.getConnection(EmbeddedPostgres.shared().jdbcUrl("postgres"))) {
            var result = Lean.run(model, c.island(), SqlIslandCases.RUNTIME, pg);
            List<List<String>> rows = new ArrayList<>();
            for (var row : result.rows()) {
                List<String> cells = new ArrayList<>();
                for (Object value : row) {
                    cells.add(SqlIslandCases.cell(value));
                }
                rows.add(cells);
            }
            assertEquals(c.columns(), result.columns().stream().map(col -> col.name()).toList(), sql);
            assertRows(c, rows);
        }
    }

    @org.junit.jupiter.api.Test
    void aNumericConstantNoDoubleHoldsIsRefusedInPostgressOwnWords() throws SQLException {
        // 2026-11-24: a numeric constant cast to a double precision is read by the island as a constant; one no double
        // holds is the error Postgres raises for it, which names the numeric's plain digits
        for (String cast : List.of("cast(1e400 as float8)", "cast(-1.5e-400 as double precision)",
                "cast(-12345e305 as float8)", "cast(1e-16384 as float8)", "cast(1e131073 as float8)")) {
            try (Connection pg = DriverManager.getConnection(EmbeddedPostgres.shared().jdbcUrl("postgres"));
                    Statement s = pg.createStatement()) {
                SQLException raised = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                        () -> s.executeQuery("select " + cast + " from EMP"), cast);
                String postgres = raised.getMessage().lines().findFirst().orElseThrow().replaceFirst("^ERROR: ", "");
                var refused = org.junit.jupiter.api.Assertions.assertThrows(
                        com.legend.error.LegendCompileException.class,
                        () -> Lean.run(model, "#SQL{select " + cast + " from " + SqlIslandCases.SOURCE + "}#",
                                SqlIslandCases.RUNTIME, pg), cast);
                assertEquals("#SQL{...}# at line 1, column 13: " + postgres, refused.getMessage(), cast);
            }
        }
    }

    private static void assertRows(SqlIslandCases.Case c, List<List<String>> rows) {
        if (c.ordered()) {
            assertEquals(c.rows(), rows, c.sql());
        } else {
            assertEquals(c.rows().stream().map(Object::toString).sorted().toList(),
                    rows.stream().map(Object::toString).sorted().toList(), c.sql());
        }
    }
}
