// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.server.QueryService;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqAnswers;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code #IQ{...}#} island's parity cases on a real Snowflake account ({@link IqParity}; the IQ scale program,
 * docs/IQ_SCALE_PROGRAM_2026_10_22.md, P1.1): each case under each of its profiles as an island over the schema's
 * tables, seeded into a schema of this run's own, judged against the snapped IQ or SQL Anywhere answer, and held to the
 * ledger's {@code snowflake} column. No new answer is snapped: the committed ones judge Snowflake as they judge DuckDB.
 *
 * <p>Its own target ({@code //core:iq_parity_snowflake_test}), manual: it needs the account's properties file
 * ({@code LEGENDLITE_PCT_CLOUD_PROPERTIES}, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §4) and a run costs warehouse time.
 * The runs go over {@code -Diq.parity.threads} sessions (default 8). {@code -Diq.parity.only=<glob>[,<glob>...]} runs
 * the runs whose key matches (a case file's, {@code lite/m1d/**}), judged against the ledger's column for those alone;
 * {@code -Diq.parity.update=<the ledger's path>} writes the column (a whole run only). Every run's status beside the
 * DuckDB column's goes to the test's outputs, {@code iq-parity-snowflake-triage.tsv}: the triage P1.2 works from.
 */
class IqParitySnowflakeTest {

    private static final String LANE = "snowflake";

    @Test
    void everyIqCaseHasItsLedgerStatusOnSnowflake() throws Exception {
        Path schemaFile = Runfile.property("iq.parity.schema");
        List<IqParity.Run> all = IqParity.runs(IqParity.cases(Runfile.property("iq.parity.lite").getParent()));
        List<PathMatcher> only = only();
        List<IqParity.Run> runs = only.isEmpty() ? all
                : all.stream().filter(r -> only.stream().anyMatch(m -> m.matches(Path.of(r.key())))).toList();
        List<SqlParity.Table> tables = IqParity.schema(schemaFile);
        Set<String> names = IqParity.tableNames(tables);
        Map<String, IqAnswers.Snap> answers = IqAnswers.read(Runfile.property("iq.parity.answers"));
        List<String> problems = Collections.synchronizedList(new ArrayList<>(
                IqAnswers.guards(all, answers, Files.readAllBytes(schemaFile))));
        TestBackend snowflake = TestBackends.of(DatabaseType.Snowflake);
        String schema = "LEGENDLITE_IQ_PARITY_" + Long.toString(System.currentTimeMillis(), 36).toUpperCase(Locale.ROOT)
                + "_" + Integer.toString((int) (Math.random() * 1_000_000), 36).toUpperCase(Locale.ROOT);
        Map<String, SqlParity.Outcome> found = new ConcurrentHashMap<>();
        try (Connection admin = snowflake.openSystem()) {
            try (Statement s = admin.createStatement()) {
                s.execute("CREATE SCHEMA " + schema);
                s.execute("USE SCHEMA " + schema);
            }
            try {
                IqParity.seed(snowflake, admin, tables);
                String model = IqParity.model(tables, snowflake.connectionBody());
                int threads = Integer.getInteger("iq.parity.threads", 8);
                ConcurrentLinkedQueue<IqParity.Run> queue = new ConcurrentLinkedQueue<>(runs);
                ExecutorService pool = Executors.newFixedThreadPool(threads);
                try {
                    List<Future<?>> workers = new ArrayList<>();
                    for (int i = 0; i < threads; i++) {
                        workers.add(pool.submit(() -> {
                            work(snowflake, schema, model, queue, answers, tables, names, found, problems);
                            return null;
                        }));
                    }
                    for (Future<?> w : workers) {
                        w.get();
                    }
                } finally {
                    pool.shutdownNow();
                }
            } finally {
                try (Statement s = admin.createStatement()) {
                    s.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
                }
            }
        }
        Map<String, SqlParity.Outcome> outcomes = new LinkedHashMap<>();
        for (IqParity.Run r : runs) {
            outcomes.put(r.key(), found.get(r.key()));
        }
        Path ledger = Runfile.property("iq.parity.ledger");
        triage(runs, outcomes, ledger);
        String update = System.getProperty("iq.parity.update");
        if (only.isEmpty()) {
            problems.addAll(IqParity.ratchet(runs, outcomes, ledger, LANE, update == null ? null : Path.of(update)));
            if (update == null) {
                problems.addAll(IqParity.divergences(runs, outcomes, Runfile.property("iq.parity.divergences"),
                        LANE));
            }
        } else {
            if (update != null) {
                problems.add("-Diq.parity.update writes the whole column: run without -Diq.parity.only");
            }
            problems.addAll(againstTheColumn(runs, outcomes, ledger));
        }
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems));
    }

    /** One worker: a session of its own on the run's schema, taking runs until none is left. */
    private static void work(TestBackend snowflake, String schema, String model,
            ConcurrentLinkedQueue<IqParity.Run> queue, Map<String, IqAnswers.Snap> answers,
            List<SqlParity.Table> tables, Set<String> names, Map<String, SqlParity.Outcome> found,
            List<String> problems) throws SQLException {
        try (Connection session = snowflake.openSystem()) {
            try (Statement s = session.createStatement()) {
                s.execute("USE SCHEMA " + schema);
                // a statement Snowflake has not answered in two minutes is the run's error, not the lane's hang
                s.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120");
            }
            QueryService service = new QueryService();
            for (IqParity.Run r = queue.poll(); r != null; r = queue.poll()) {
                String mismatch = IqParity.engineMismatch(r, names);
                if (mismatch != null) {
                    problems.add(mismatch);
                }
                SqlParity.Outcome outcome = IqParity.outcome(r, answers.get(r.key()), tables, names,
                        island -> service.execute(model, island, IqParity.RUNTIME, session));
                found.put(r.key(), outcome);
                String nondeterministic = IqParity.determinism(r, tables, outcome.status());
                if (nondeterministic != null) {
                    problems.add(nondeterministic);
                }
            }
        }
    }

    /** {@code -Diq.parity.only}'s globs over the runs' keys; none, every run. */
    private static List<PathMatcher> only() {
        String only = System.getProperty("iq.parity.only", "").strip();
        List<PathMatcher> out = new ArrayList<>();
        for (String glob : only.isEmpty() ? new String[0] : only.split(",")) {
            out.add(FileSystems.getDefault().getPathMatcher("glob:" + glob.strip()));
        }
        return out;
    }

    /** The selected runs against the ledger's Snowflake column: a run with no status there is NEW, one whose status
     *  differs is a REGRESSION, a FIX or a MISMATCH, as the whole column's ratchet says them. */
    private static List<String> againstTheColumn(List<IqParity.Run> runs, Map<String, SqlParity.Outcome> outcomes,
            Path ledger) throws java.io.IOException {
        Map<String, String[]> pinned = SqlParity.readLedger(ledger, IqParity.LANES.size());
        int column = IqParity.LANES.indexOf(LANE);
        List<String> problems = new ArrayList<>();
        for (IqParity.Run r : runs) {
            String key = r.key();
            String[] statuses = pinned.get(key);
            String was = statuses == null ? null : statuses[column];
            String now = outcomes.get(r.key()).status().name();
            if (was == null || was.isEmpty()) {
                problems.add("NEW: " + key + " is " + now + " and not in the ledger's " + LANE + " column");
            } else if (!was.equals(now)) {
                problems.add((was.equals("PASS") ? "REGRESSION" : now.equals("PASS") ? "FIX DETECTED"
                        : "STATUS MISMATCH") + ": " + key + " was " + was + ", now " + now);
            }
        }
        return problems;
    }

    /** Each run's Snowflake status beside the DuckDB column's, every run that differs with its detail, to the test's
     *  outputs; the transitions' counts to standard out. */
    private static void triage(List<IqParity.Run> runs, Map<String, SqlParity.Outcome> outcomes, Path ledger)
            throws java.io.IOException {
        Map<String, String[]> pinned = SqlParity.readLedger(ledger, IqParity.LANES.size());
        int duckdb = IqParity.LANES.indexOf("duckdb");
        Map<String, Integer> moves = new TreeMap<>();
        StringBuilder out = new StringBuilder("case\tduckdb\tsnowflake\tdetail\n");
        for (IqParity.Run r : runs) {
            String key = r.key();
            String[] statuses = pinned.get(key);
            String duck = statuses == null || statuses[duckdb] == null ? "" : statuses[duckdb];
            SqlParity.Outcome o = outcomes.get(r.key());
            String now = o.status().name();
            moves.merge(duck + " -> " + now, 1, Integer::sum);
            if (!duck.equals(now)) {
                out.append(key).append('\t').append(duck).append('\t').append(now).append('\t')
                        .append(o.detail()).append('\n');
            }
        }
        System.out.println("iq-parity, duckdb column -> snowflake lane: " + moves);
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "iq-parity-snowflake-triage.tsv"), out.toString(),
                    StandardCharsets.UTF_8);
        }
    }
}
