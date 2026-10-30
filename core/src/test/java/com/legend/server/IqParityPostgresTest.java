// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.testing.Lean;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqAnswers;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;
import com.legend.testing.EmbeddedPostgres;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
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
 * The {@code #IQ{...}#} island's parity cases on Postgres 16 ({@link IqParity}): each case under each of its profiles
 * as an island over the schema's tables in the embedded Postgres, judged against SQL Anywhere 17's snapped answer (or
 * the case's provisional decision), and held to the ledger's {@code postgres} column. Unlike sql-parity's Postgres
 * lane it has no drift step: its referee is SQL Anywhere, which only {@code tools/iq-oracle} reaches (locally); the
 * answers file is checked against the cases (UNSNAPPED, STALE, GONE) instead.
 *
 * <p>Its own target ({@code //core:iq_parity_postgres_test}), as {@code SqlParityPostgresTest}: it needs the embedded
 * Postgres. {@code --jvm_flag=-Diq.parity.update=<the ledger's path>} writes the ledger's column.
 */
class IqParityPostgresTest {

    @Test
    void everyIqCaseHasItsLedgerStatusOnPostgres() throws Exception {
        Path schemaFile = Runfile.property("iq.parity.schema");
        List<IqParity.Run> runs = IqParity.runs(IqParity.cases(Runfile.property("iq.parity.lite").getParent()));
        List<SqlParity.Table> tables = IqParity.schema(schemaFile);
        Set<String> names = IqParity.tableNames(tables);
        Map<String, IqAnswers.Snap> answers = IqAnswers.read(Runfile.property("iq.parity.answers"));
        List<String> problems = new ArrayList<>(IqAnswers.guards(runs, answers, Files.readAllBytes(schemaFile)));
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl("postgres"))) {
            IqParity.seed(TestBackends.of(DatabaseType.Postgres), admin, tables);
            grantToThisAccount(admin, tables);
            String model = IqParity.model(tables, String.format(Locale.ROOT, "type: Postgres;\n    specification:"
                    + " Static { host: '127.0.0.1'; port: %d; name: 'postgres'; };\n    auth: Test;", pg.port()));
            Map<String, SqlParity.Outcome> outcomes = new LinkedHashMap<>();
            for (IqParity.Run r : runs) {
                String mismatch = IqParity.engineMismatch(r, names);
                if (mismatch != null) {
                    problems.add(mismatch);
                }
                SqlParity.Outcome outcome = IqParity.outcome(r, answers.get(r.key()), tables, names,
                        island -> Lean.run(model, island, IqParity.RUNTIME, admin));
                outcomes.put(r.key(), outcome);
                // IQ island, 2026-10-14 (review 7, the user's ruling on B1): no case pins one plan's choice
                String nondeterministic = IqParity.determinism(r, tables, outcome.status());
                if (nondeterministic != null) {
                    problems.add(nondeterministic);
                }
            }
            String update = System.getProperty("iq.parity.update");
            problems.addAll(IqParity.ratchet(runs, outcomes, Runfile.property("iq.parity.ledger"), "postgres",
                    update == null ? null : Path.of(update)));
            if (update == null) {
                // IQ island, landing 4 (WP1), 2026-10-14: every FAIL, BUG or ERROR is a listed iq17 divergence, and
                // the list only shrinks
                problems.addAll(IqParity.divergences(runs, outcomes, Runfile.property("iq.parity.divergences"),
                        "postgres"));
            }
        }
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems));
    }

    /** A connection with {@code auth: Test} logs in as this operating-system account (as SqlParityPostgresTest): it
     *  needs a login role that reads the tables. */
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
