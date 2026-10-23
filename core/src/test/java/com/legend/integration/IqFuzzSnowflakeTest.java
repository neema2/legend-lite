// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.server.QueryService;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqFuzz;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
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
 * The fuzzer's differential between lite's two targets (the IQ scale program, docs/IQ_SCALE_PROGRAM_2026_10_22.md
 * P2.2): random IQ queries ({@link IqFuzz}) as islands, answered by in-process DuckDB and by a real Snowflake account
 * over the same seeded schema, their rows compared as multisets ({@link SqlParity#compare}). No IQ answer is needed:
 * the two must agree. A finding is a query the two answer differently (DIFF), one DuckDB answers and Snowflake fails
 * (SF_ERROR), or one where only DuckDB raises (ONLY_DUCKDB_RAISES: IQ's error emulated in one target alone). A
 * refusal by name on Snowflake alone (SF_REFUSED) is counted and ranked, not a finding.
 *
 * <p>Manual ({@code //core:iq_fuzz_snowflake_test}): it needs the account's properties file
 * ({@code LEGENDLITE_PCT_CLOUD_PROPERTIES}), and a run costs warehouse time. {@code -Diq.fuzz.seed} (default 1),
 * {@code -Diq.fuzz.count} (shapes, default 300; three queries each), {@code -Diq.fuzz.depth} (default 3),
 * {@code -Diq.fuzz.profile} (default {@code iq17-default}), {@code -Diq.parity.threads} (Snowflake sessions, default
 * 8). The findings and Snowflake's refusals go to the test's outputs.
 */
class IqFuzzSnowflakeTest {

    private static final TestBackend PLATFORM = TestBackends.platform();

    /** One query of the run: its seed, its form and its SQL. */
    private record Query(long seed, int q, String form, String sql) {
    }

    /** One target's answer: its kind ({@link IqFuzzTest#classify}'s, or ROWS) and its rows or its reason. */
    private record Answer(String kind, SqlParity.@com.legend.base.Nullable Grid grid, String reason) {
    }

    @Test
    void duckDbAndSnowflakeAnswerRandomIslandsAlike() throws Exception {
        long seed = Long.getLong("iq.fuzz.seed", 1L);
        int count = Integer.getInteger("iq.fuzz.count", 300);
        int depth = Integer.getInteger("iq.fuzz.depth", 3);
        String profile = System.getProperty("iq.fuzz.profile", "iq17-default");
        int threads = Integer.getInteger("iq.parity.threads", 8);
        List<SqlParity.Table> tables = IqParity.schema(Runfile.property("iq.parity.schema"));
        Set<String> names = IqParity.tableNames(tables);
        List<Query> queries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long s = seed * 1_000_003L + i;
            IqFuzz gen = new IqFuzz(s, tables, depth);
            IqFuzz.Shape shape = gen.shape();
            String p = gen.predicate(shape.scope());
            queries.add(new Query(s, 0, "shape", shape.sql()));
            queries.add(new Query(s, 1, "where", shape.where("(" + p + ")")));
            queries.add(new Query(s, 4, "complex", gen.complex()));
        }
        // DuckDB first, in process: what it refuses never reaches the account
        Map<Query, Answer> duck = new java.util.LinkedHashMap<>();
        try (Connection session = PLATFORM.open()) {
            IqParity.seed(PLATFORM, session, tables);
            String model = IqParity.model(tables, PLATFORM.connectionBody());
            QueryService service = new QueryService();
            for (Query q : queries) {
                duck.put(q, answer(service, session, model, q, profile, tables, names));
            }
        }
        TestBackend snowflake = TestBackends.of(DatabaseType.Snowflake);
        String model = IqParity.model(tables, snowflake.connectionBody());
        String schema = "LEGENDLITE_IQ_FUZZ_" + Long.toString(System.currentTimeMillis(), 36).toUpperCase(Locale.ROOT)
                + "_" + Integer.toString((int) (Math.random() * 1_000_000), 36).toUpperCase(Locale.ROOT);
        Map<Query, Answer> snow = new ConcurrentHashMap<>();
        try (Connection admin = snowflake.openSystem()) {
            try (Statement s = admin.createStatement()) {
                s.execute("CREATE SCHEMA " + schema);
                s.execute("USE SCHEMA " + schema);
            }
            try {
                IqParity.seed(snowflake, admin, tables);
                ConcurrentLinkedQueue<Query> queue = new ConcurrentLinkedQueue<>();
                duck.forEach((q, a) -> {
                    if (!a.kind().equals("REFUSED")) {
                        queue.add(q);
                    }
                });
                ExecutorService pool = Executors.newFixedThreadPool(threads);
                try {
                    List<Future<?>> workers = new ArrayList<>();
                    for (int i = 0; i < threads; i++) {
                        workers.add(pool.submit(() -> {
                            work(snowflake, schema, model, queue, profile, tables, names, snow);
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
        report(seed, count, depth, profile, duck, snow);
    }

    /** One worker: a Snowflake session of its own on the run's schema, taking queries until none is left. */
    private static void work(TestBackend snowflake, String schema, String model, ConcurrentLinkedQueue<Query> queue,
            String profile, List<SqlParity.Table> tables, Set<String> names, Map<Query, Answer> snow)
            throws Exception {
        try (Connection session = snowflake.openSystem()) {
            try (Statement s = session.createStatement()) {
                s.execute("USE SCHEMA " + schema);
                // a statement Snowflake has not answered in two minutes is the query's error, not the run's hang
                s.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120");
            }
            QueryService service = new QueryService();
            for (Query q = queue.poll(); q != null; q = queue.poll()) {
                snow.put(q, answer(service, session, model, q, profile, tables, names));
            }
        }
    }

    private static Answer answer(QueryService service, Connection session, String model, Query q, String profile,
            List<SqlParity.Table> tables, Set<String> names) {
        try {
            var result = service.execute(model, IqFuzzTest.island(q.sql(), profile, tables, names, q.seed(), q.q()),
                    IqParity.RUNTIME, session);
            return new Answer("ROWS", SqlParity.grid(result), "");
        } catch (Exception | StackOverflowError e) {
            IqFuzzTest.Outcome o = IqFuzzTest.classify(e);
            return new Answer(o.kind(), null, o.reason());
        }
    }

    /** Each query's pair of answers, judged; the counts to standard out, the findings and Snowflake's refusals to the
     *  test's outputs. */
    private static void report(long seed, int count, int depth, String profile, Map<Query, Answer> duck,
            Map<Query, Answer> snow) throws java.io.IOException {
        Map<String, Integer> pairs = new TreeMap<>();
        Map<String, Integer> refusals = new TreeMap<>();
        StringBuilder findings = new StringBuilder("seed\tform\tverdict\tsql\tduckdb\tsnowflake\n");
        int found = 0;
        for (var e : duck.entrySet()) {
            Query q = e.getKey();
            Answer d = e.getValue();
            Answer s = snow.get(q);
            String verdict = verdict(d, s);
            pairs.merge(verdict, 1, Integer::sum);
            if (verdict.equals("SF_REFUSED")) {
                refusals.merge(s.reason().replaceAll("\\d+", "N"), 1, Integer::sum);
            }
            if (verdict.equals("DIFF") || verdict.equals("SF_ERROR") || verdict.equals("ONLY_DUCKDB_RAISES")) {
                found++;
                String what = verdict.equals("DIFF") ? String.join("; ", SqlParity.compare(false, null, d.grid(),
                        s.grid())) : s.kind() + " " + s.reason();
                findings.append(q.seed()).append('\t').append(q.form()).append('\t').append(verdict).append('\t')
                        .append(q.sql()).append('\t').append(d.kind().equals("ROWS") ? d.grid().rows().size()
                                + " rows" : d.kind() + " " + d.reason()).append('\t').append(oneLine(what))
                        .append('\n');
            }
        }
        StringBuilder ranked = new StringBuilder("count\treason\n");
        refusals.entrySet().stream().sorted((a, b) -> b.getValue() - a.getValue())
                .forEach(r -> ranked.append(r.getValue()).append('\t').append(r.getKey()).append('\n'));
        System.out.println("iq-fuzz-snowflake seed " + seed + ", " + count + " shapes, depth " + depth + ", " + profile
                + ": " + pairs + "; " + found + " findings");
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "iq-fuzz-snowflake.tsv"), findings.toString(), StandardCharsets.UTF_8);
            Files.writeString(Path.of(outputs, "iq-fuzz-snowflake-refusals.tsv"), ranked.toString(),
                    StandardCharsets.UTF_8);
        }
        assertTrue(found == 0, found + " findings (the test's outputs, iq-fuzz-snowflake.tsv):\n"
                + findings.substring(0, Math.min(findings.length(), 20_000)));
    }

    /** The pair's verdict: REFUSED (DuckDB refused; never sent), MATCH, DIFF, SF_REFUSED, SF_ERROR,
     *  ONLY_DUCKDB_RAISES or BOTH_RAISE. */
    private static String verdict(Answer d, @com.legend.base.Nullable Answer s) {
        if (s == null) {
            return "REFUSED";
        }
        boolean dRows = d.kind().equals("ROWS");
        boolean sRows = s.kind().equals("ROWS");
        if (dRows && sRows) {
            return SqlParity.compare(false, null, d.grid(), s.grid()).isEmpty() ? "MATCH" : "DIFF";
        }
        if (dRows) {
            return s.kind().equals("REFUSED") ? "SF_REFUSED" : "SF_ERROR";
        }
        return sRows ? "ONLY_DUCKDB_RAISES" : "BOTH_RAISE";
    }

    private static String oneLine(String s) {
        String flat = s.replaceAll("\\s+", " ");
        return flat.length() > 600 ? flat.substring(0, 600) + "..." : flat;
    }
}
