// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.testing.Lean;

import com.legend.testcases.PostgresSeed;
import com.legend.testcases.SqlParity;
import com.legend.testing.EmbeddedPostgres;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Upstream's Postgres parity corpus on Postgres 16 ({@link SqlParity}), in two steps. First the snapped answers are
 * checked against Postgres itself: each case's SQL run as written over the corpus's tables must answer what the
 * answers file says, so the file cannot drift from Postgres. Then each case runs as a {@code #SQL{...}#} island through
 * the Postgres dialect, judged by upstream's rules and held to the ledger's {@code postgres} column.
 *
 * <p>Its own target ({@code //core:sql_parity_postgres_test}), as {@code SqlIslandPostgresTest}: it needs the embedded
 * Postgres and the pinned legend-engine tree. {@code --jvm_flag=-Dsql.parity.snap=<the answers file's path>} writes
 * the answers from Postgres; {@code --jvm_flag=-Dsql.parity.update=<the ledger's path>} writes the ledger's column.
 */
class SqlParityPostgresTest {

    @Test
    void theAnswersArePostgresAndEveryCaseHasItsLedgerStatus() throws Exception {
        Path dir = Runfile.property("sql.parity.schema").getParent();
        List<SqlParity.Case> cases = new java.util.ArrayList<>(SqlParity.cases(dir));
        cases.addAll(SqlParity.cases(Runfile.property("sql.parity.lite").getParent(), "lite/"));
        List<SqlParity.Table> tables = SqlParity.schema(dir);
        Set<String> names = SqlParity.tableNames(tables);
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        List<String> problems = new ArrayList<>();
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl("postgres"))) {
            PostgresSeed.seed(admin, tables);
            grantToThisAccount(admin, tables);

            // the answers are Postgres's
            Map<String, SqlParity.Answer> live = new LinkedHashMap<>();
            for (SqlParity.Case c : cases) {
                if (c.skip() == null && c.check() != null) {
                    // a statement that writes a table, and the query that reads what it wrote, in a transaction
                    // rolled back after
                    admin.setAutoCommit(false);
                    try {
                        SqlParity.Answer written = written(admin, c.sql());
                        live.put(SqlParity.key(c), written);
                        if (written instanceof SqlParity.Rows) {
                            live.put(SqlParity.key(c) + SqlParity.CHECK, answer(admin, c.check()));
                        }
                    } finally {
                        admin.rollback();
                        admin.setAutoCommit(true);
                    }
                } else if (c.skip() == null) {
                    live.put(SqlParity.key(c), answer(admin, c.sql()));
                }
            }
            String snap = System.getProperty("sql.parity.snap");
            Map<String, SqlParity.Answer> answers;
            if (snap != null) {
                SqlParity.writeAnswers(Path.of(snap), cases, live);
                System.out.println("wrote " + snap);
                answers = live;
            } else {
                answers = SqlParity.readAnswers(Runfile.property("sql.parity.answers"));
                for (Map.Entry<String, SqlParity.Answer> e : live.entrySet()) {
                    SqlParity.Answer snapped = answers.get(e.getKey());
                    if (snapped == null) {
                        problems.add("UNSNAPPED: " + e.getKey() + " has no answer in the answers file");
                    } else if (!SqlParity.agrees(snapped, e.getValue())) {
                        problems.add("DRIFT: " + e.getKey() + ": the file says " + SqlParity.encoded(snapped)
                                + ", Postgres says " + SqlParity.encoded(e.getValue()));
                    }
                }
            }

            // each island through the Postgres dialect
            String model = SqlParity.model(tables, String.format(Locale.ROOT, "type: Postgres;\n    specification:"
                    + " Static { host: '127.0.0.1'; port: %d; name: 'postgres'; };\n    auth: Test;", pg.port()));
            Map<String, SqlParity.Outcome> outcomes = new LinkedHashMap<>();
            SqlParity.Legend legend = new SqlParity.Legend() {
                @Override
                public Lean.Result run(String island) throws Exception {
                    return Lean.run(model, island, SqlParity.RUNTIME, admin);
                }

                @Override
                public void begin() throws Exception {
                    admin.setAutoCommit(false);
                }

                @Override
                public void rollback() throws Exception {
                    admin.rollback();
                    admin.setAutoCommit(true);
                }
            };
            for (SqlParity.Case c : cases) {
                outcomes.put(SqlParity.key(c), SqlParity.outcome(c, answers, names, legend));
            }
            String update = System.getProperty("sql.parity.update");
            problems.addAll(SqlParity.ratchet(cases, outcomes, Runfile.property("sql.parity.ledger"), "postgres",
                    update == null ? null : Path.of(update)));
        }
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems));
    }

    /** What Postgres answers {@code sql}, a statement that writes a table: its count of rows (one column,
     *  {@code count}), the rows it returns (RETURNING), or its error; a failed statement leaves the transaction to its
     *  rollback. */
    private static SqlParity.Answer written(Connection pg, String sql) {
        try (Statement s = pg.createStatement()) {
            if (s.execute(sql)) {
                // (RETURNING: the rows it returns)
                try (ResultSet rs = s.getResultSet()) {
                    return new SqlParity.Rows(SqlParity.grid(rs));
                }
            }
            long count = s.getLargeUpdateCount();
            return new SqlParity.Rows(new SqlParity.Grid(List.of("count"), List.of(List.of((Object) count))));
        } catch (SQLException e) {
            String message = e.getMessage() == null ? "" : e.getMessage().split("\n")[0];
            return new SqlParity.Rejected(e.getSQLState() == null ? "" : e.getSQLState(), message);
        }
    }

    /** What Postgres answers {@code sql}: its rows, or its error. */
    private static SqlParity.Answer answer(Connection pg, String sql) {
        try (Statement s = pg.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return new SqlParity.Rows(SqlParity.grid(rs));
        } catch (SQLException e) {
            String message = e.getMessage() == null ? "" : e.getMessage().split("\n")[0];
            return new SqlParity.Rejected(e.getSQLState() == null ? "" : e.getSQLState(), message);
        }
    }

    /** A connection with {@code auth: Test} logs in as this operating-system account (as SqlIslandPostgresTest):
     *  it needs a login role that reads the tables. */
    private static void grantToThisAccount(Connection admin, List<SqlParity.Table> tables) throws SQLException {
        String user = System.getProperty("user.name");
        String account = "\"" + user.replace("\"", "\"\"") + "\"";
        try (PreparedStatement exists = admin.prepareStatement("SELECT 1 FROM pg_roles WHERE rolname = ?")) {
            exists.setString(1, user);
            try (ResultSet r = exists.executeQuery(); Statement s = admin.createStatement()) {
                if (!r.next()) {
                    s.execute("CREATE ROLE " + account + " LOGIN");
                }
                for (SqlParity.Table t : tables) {
                    s.execute("GRANT SELECT ON " + t.name() + " TO " + account);
                }
            }
        }
    }
}
