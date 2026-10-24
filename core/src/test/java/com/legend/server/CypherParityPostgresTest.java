// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.exec.ExecutionResult;
import com.legend.testcases.CypherAnswers;
import com.legend.testcases.CypherGraph;
import com.legend.testcases.CypherParity;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Cypher parity corpus on the embedded Postgres 16 (docs/CYPHER_STUDY_2026_10_15.md §8.4): the DuckDB lane's
 * cases, graphs, answers and judge ({@code CypherParityTest}), each case's status pinned in the ledger's
 * {@code postgres} column. The two columns agree where lite's answer is the database's; where a lane differs, the
 * ledger shows it. Run with {@code -Dcypher.parity.update=<ledger in the workspace>} to rewrite this column.
 */
class CypherParityPostgresTest {

    @Test
    void everyCaseHasItsLedgerStatusOnPostgres() throws Exception {
        List<CypherParity.Case> cases = CypherParity.cases(Runfile.property("cypher.parity.tck").getParent(),
                Runfile.property("cypher.parity.lite").getParent());
        // a development run over some of the cases (-Dcypher.parity.only=<a regex found in a case's key>): each
        // case's outcome printed, the ledger not read (as CypherParityTest's)
        String only = System.getProperty("cypher.parity.only");
        if (only != null) {
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(only);
            cases = cases.stream().filter(c -> p.matcher(c.key()).find()).toList();
        }
        CypherAnswers.File answers = CypherAnswers.read(Runfile.property("cypher.parity.answers"));
        Map<String, CypherParity.Decision> decisions = CypherParity.decisions(
                Runfile.property("cypher.parity.decisions"), "postgres");
        List<String> problems = new ArrayList<>(CypherAnswers.guards(cases, answers, CypherParity.PINNED_SERVER));
        problems.addAll(CypherParity.decisionGuards(cases, Runfile.property("cypher.parity.decisions")));
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        Map<String, SqlParity.Outcome> outcomes;
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl("postgres"))) {
            ensureThisAccount(admin);
            QueryService service = new QueryService();
            String connection = String.format(Locale.ROOT, "type: Postgres;\n    specification: Static { host:"
                    + " '127.0.0.1'; port: %d; name: 'postgres'; };\n    auth: Test;", pg.port());
            outcomes = CypherParity.run(cases, answers.answers(), decisions, new CypherParity.Lane() {
                @Override
                public String connection() {
                    return connection;
                }

                @Override
                public void seed(List<SqlParity.Table> tables) throws Exception {
                    PostgresSeed.seed(admin, tables);
                    grantToThisAccount(admin, tables);
                }

                @Override
                public ExecutionResult run(String model, String island) throws Exception {
                    return service.execute(model, island, CypherGraph.RUNTIME, admin);
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
            }, problems);
        }
        if (only != null) {
            outcomes.forEach((k, o) -> System.out.println(o.status() + "\t" + k + "\t" + o.detail()));
            return;
        }
        CypherParity.census(outcomes, "postgres");
        System.out.println("the write cases by what a graph over a domain model needs: "
                + CypherParity.writeGroups(cases, answers.answers()));
        String update = System.getProperty("cypher.parity.update");
        problems.addAll(CypherParity.ratchet(cases, outcomes, Runfile.property("cypher.parity.ledger"), "postgres",
                update == null ? null : Path.of(update)));
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems.subList(0,
                Math.min(problems.size(), 200))));
    }

    /** A connection with {@code auth: Test} logs in as this operating-system account (as the IQ lane does): it
     *  needs a login role. */
    private static void ensureThisAccount(Connection admin) throws SQLException {
        String user = System.getProperty("user.name");
        String account = "\"" + user.replace("\"", "\"\"") + "\"";
        try (PreparedStatement exists = admin.prepareStatement("SELECT 1 FROM pg_roles WHERE rolname = ?")) {
            exists.setString(1, user);
            try (ResultSet r = exists.executeQuery(); Statement s = admin.createStatement()) {
                if (!r.next()) {
                    s.execute("CREATE ROLE " + account + " LOGIN");
                }
                s.execute("GRANT USAGE ON SCHEMA public TO " + account);
            }
        }
    }

    /** The account reads every table the lane just seeded. */
    private static void grantToThisAccount(Connection admin, List<SqlParity.Table> tables) throws SQLException {
        String user = System.getProperty("user.name");
        String account = "\"" + user.replace("\"", "\"\"") + "\"";
        try (Statement s = admin.createStatement()) {
            for (SqlParity.Table t : tables) {
                s.execute("GRANT SELECT ON " + t.name() + " TO " + account);
            }
        }
    }
}
