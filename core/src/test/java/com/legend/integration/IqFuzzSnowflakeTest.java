// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.server.QueryService;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The fuzzer's differential between lite's two targets (the IQ scale program, docs/IQ_SCALE_PROGRAM_2026_10_22.md
 * P2.2): random IQ queries ({@link IqFuzz}) as islands, answered by in-process DuckDB and by a real Snowflake account
 * over the same seeded schema, their rows compared as multisets ({@link SqlParity#compare}). No IQ answer is needed:
 * the two must agree ({@link IqFuzzDifferential}: its verdicts, Snowflake the other target).
 *
 * <p>Manual ({@code //core:iq_fuzz_snowflake_test}): it needs the account's properties file
 * ({@code LEGENDLITE_PCT_CLOUD_PROPERTIES}), and a run costs warehouse time. {@code -Diq.fuzz.seed} (default 1),
 * {@code -Diq.fuzz.count} (shapes, default 300; three queries each), {@code -Diq.fuzz.depth} (default 3),
 * {@code -Diq.fuzz.profile} (default {@code iq17-default}), {@code -Diq.parity.threads} (Snowflake sessions, default
 * 8). The findings and Snowflake's refusals go to the test's outputs.
 */
class IqFuzzSnowflakeTest {

    @Test
    void thePlatformAndSnowflakeAnswerRandomIslandsAlike() throws Exception {
        long seed = Long.getLong("iq.fuzz.seed", 1L);
        int count = Integer.getInteger("iq.fuzz.count", 300);
        int depth = Integer.getInteger("iq.fuzz.depth", 3);
        String profile = System.getProperty("iq.fuzz.profile", "iq17-default");
        int threads = Integer.getInteger("iq.parity.threads", 8);
        List<SqlParity.Table> tables = IqParity.schema(Runfile.property("iq.parity.schema"));
        Set<String> names = IqParity.tableNames(tables);
        List<IqFuzzDifferential.Query> queries = IqFuzzDifferential.queries(seed, count, depth, tables);
        // DuckDB first, in process: what it refuses never reaches the account
        Map<IqFuzzDifferential.Query, IqFuzzDifferential.Answer> duck = IqFuzzDifferential.onPlatform(queries, profile,
                tables, names);
        TestBackend snowflake = TestBackends.of(DatabaseType.Snowflake);
        String model = IqParity.model(tables, snowflake.connectionBody());
        String schema = "LEGENDLITE_IQ_FUZZ_" + Long.toString(System.currentTimeMillis(), 36).toUpperCase(Locale.ROOT)
                + "_" + Integer.toString((int) (Math.random() * 1_000_000), 36).toUpperCase(Locale.ROOT);
        Map<IqFuzzDifferential.Query, IqFuzzDifferential.Answer> snow = new ConcurrentHashMap<>();
        try (Connection admin = snowflake.openSystem()) {
            try (Statement s = admin.createStatement()) {
                s.execute("CREATE SCHEMA " + schema);
                s.execute("USE SCHEMA " + schema);
            }
            try {
                IqParity.seed(snowflake, admin, tables);
                ConcurrentLinkedQueue<IqFuzzDifferential.Query> queue = new ConcurrentLinkedQueue<>();
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
        IqFuzzDifferential.report("snowflake", seed, count, depth, profile, duck, snow);
    }

    /** One worker: a Snowflake session of its own on the run's schema, taking queries until none is left. */
    private static void work(TestBackend snowflake, String schema, String model,
            ConcurrentLinkedQueue<IqFuzzDifferential.Query> queue, String profile, List<SqlParity.Table> tables,
            Set<String> names, Map<IqFuzzDifferential.Query, IqFuzzDifferential.Answer> snow) throws Exception {
        try (Connection session = snowflake.openSystem()) {
            try (Statement s = session.createStatement()) {
                s.execute("USE SCHEMA " + schema);
                // a statement Snowflake has not answered in two minutes is the query's error, not the run's hang
                s.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120");
            }
            QueryService service = new QueryService();
            for (IqFuzzDifferential.Query q = queue.poll(); q != null; q = queue.poll()) {
                snow.put(q, IqFuzzDifferential.answer(service, session, model, q, profile, tables, names));
            }
        }
    }
}
