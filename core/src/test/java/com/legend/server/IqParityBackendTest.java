// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqAnswers;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code #IQ{...}#} island's parity cases on one of the other databases the platform executes on (IQ island,
 * landing 6, 2026-10-22: the user's "translation to other backends"): each case under each of its profiles as an
 * island over the schema's tables in that database, judged against SQL Anywhere 17's or the live IQ 17.1's snapped
 * answer exactly as the DuckDB and Postgres lanes judge it, and held to the other backends' ledger
 * ({@code iq-parity/backends.tsv}), the lane's column. The lane is {@code -Diq.parity.backend=<name>}, one of
 * {@link IqParity#BACKEND_LANES}, a {@link TestBackend}'s name: SQLite and H2 in-process, MariaDB, MySQL, ClickHouse and
 * Trino on their embedded servers.
 *
 * <p>Every FAIL, BUG or ERROR is a listed divergence of the lane, as on DuckDB and Postgres: a run SQL Anywhere judges
 * never diverges (fix it, or refuse it by name on that database), an {@code iq17-*} run only for a cause the
 * divergence file names. {@code -Diq.parity.update=<the ledger's path>} writes the lane's column instead.
 */
class IqParityBackendTest {

    @Test
    void everyIqCaseHasItsLedgerStatusOnTheBackend() throws Exception {
        String lane = System.getProperty("iq.parity.backend");
        if (lane == null || !IqParity.BACKEND_LANES.contains(lane)) {
            throw new IllegalStateException("-Diq.parity.backend is one of " + IqParity.BACKEND_LANES + ", not " + lane);
        }
        TestBackend backend = TestBackends.named(lane);
        Path schemaFile = Runfile.property("iq.parity.schema");
        List<IqParity.Run> runs = IqParity.runs(IqParity.cases(Runfile.property("iq.parity.lite").getParent()));
        List<SqlParity.Table> tables = IqParity.schema(schemaFile);
        Set<String> names = IqParity.tableNames(tables);
        Map<String, IqAnswers.Snap> answers = IqAnswers.read(Runfile.property("iq.parity.answers"));
        List<String> problems = new ArrayList<>(IqAnswers.guards(runs, answers, Files.readAllBytes(schemaFile)));
        String model = IqParity.model(tables, backend.connectionBody());
        Map<String, SqlParity.Outcome> outcomes = new LinkedHashMap<>();
        String only = System.getProperty("iq.parity.only");
        // ClickHouse keeps a table of its own only in an HTTP session, as a temporary one: the lane runs on a session
        // connection (the system connection's kind) and its tables are temporary
        boolean sessionTables = backend.type() == com.legend.model.ConnectionDefinition.DatabaseType.ClickHouse;
        try (Connection session = sessionTables ? backend.openSystem() : backend.open()) {
            // the session the dialect's spellings assume (MariaDB's ANSI_QUOTES, its time zone ...), as the platform
            // sets its own sessions up (exec.SystemDatabase)
            try (java.sql.Statement setup = session.createStatement()) {
                for (String statement : backend.dialect().sessionSetup()) {
                    setup.execute(statement);
                }
            }
            IqParity.seed(backend, session, tables, sessionTables);
            QueryService service = new QueryService();
            for (IqParity.Run r : runs) {
                if (only != null && !r.key().contains(only)) {
                    continue;
                }
                SqlParity.Outcome outcome = IqParity.outcome(r, answers.get(r.key()), tables, names,
                        island -> service.execute(model, island, IqParity.RUNTIME, session));
                outcomes.put(r.key(), outcome);
            }
        }
        if (only != null) {
            outcomes.forEach((k, o) -> System.out.println("[iq-parity] " + lane + " " + o.status() + " " + k
                    + (o.detail().isEmpty() ? "" : ": " + o.detail())));
            return;
        }
        String update = System.getProperty("iq.parity.update");
        problems.addAll(IqParity.backendRatchet(runs, outcomes, Runfile.property("iq.parity.backends"), lane,
                update == null ? null : Path.of(update)));
        if (update == null) {
            problems.addAll(IqParity.divergences(runs, outcomes, Runfile.property("iq.parity.divergences"), lane));
        }
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems));
    }
}
