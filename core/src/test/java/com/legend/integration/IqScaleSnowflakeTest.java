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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snowflake's scale limits (the IQ scale program's Q7, docs/IQ_SCALE_PROGRAM_2026_10_22.md P1.4): the scale probe's
 * shapes ({@link IqFuzzTest#scaleShapes}: long unions, join chains, deep nesting, long IN lists and ANDs, wide
 * selects, nested derived tables and CTEs, long sums and divisions) planned for and run on a real account, each with
 * its plan time, its SQL's size, its run time and its outcome ({@code iq-scale-snowflake.tsv} in the test's outputs).
 * A limit of Snowflake's is reported, as the limit it is; only lite's own crash fails it.
 *
 * <p>Manual ({@code //core:iq_scale_snowflake_test}): it needs the account's properties file
 * ({@code LEGENDLITE_PCT_CLOUD_PROPERTIES}), and a run costs warehouse time. {@code -Diq.scale.factor} (default 1),
 * {@code -Diq.scale.only} (a regex of the shapes' names), {@code -Diq.scale.timeout} (seconds a statement may run,
 * default 300).
 */
class IqScaleSnowflakeTest {

    @Test
    void theScaleShapesOnSnowflake() throws Throwable {
        // on the server's request stack (LegendHttpServer.REQUEST_STACK_BYTES), as a query the product runs
        Throwable[] failed = new Throwable[1];
        Thread t = new Thread(null, () -> {
            try {
                shapes();
            } catch (Throwable x) {
                failed[0] = x;
            }
        }, "iq-scale-snowflake", com.legend.server.LegendHttpServer.REQUEST_STACK_BYTES);
        t.start();
        t.join();
        if (failed[0] != null) {
            throw failed[0];
        }
    }

    private static void shapes() throws Exception {
        int factor = Integer.getInteger("iq.scale.factor", 1);
        int timeout = Integer.getInteger("iq.scale.timeout", 300);
        String only = System.getProperty("iq.scale.only");
        String profile = "iq17-default";
        List<SqlParity.Table> tables = IqParity.schema(Runfile.property("iq.parity.schema"));
        Set<String> names = IqParity.tableNames(tables);
        Map<String, String> shapes = IqFuzzTest.scaleShapes(factor);
        if (only != null) {
            shapes.keySet().removeIf(k -> !k.matches(only));
        }
        TestBackend snowflake = TestBackends.of(DatabaseType.Snowflake);
        String model = IqParity.model(tables, snowflake.connectionBody());
        var compiled = com.legend.Compiler.compileModel(model);
        String schema = "LEGENDLITE_IQ_SCALE_" + Long.toString(System.currentTimeMillis(), 36).toUpperCase(Locale.ROOT);
        StringBuilder report = new StringBuilder("shape\toutcome\tplan ms\tsql chars\trun ms\tdetail\n");
        List<String> crashes = new ArrayList<>();
        try (Connection session = snowflake.openSystem()) {
            try (Statement s = session.createStatement()) {
                s.execute("CREATE SCHEMA " + schema);
                s.execute("USE SCHEMA " + schema);
                s.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = " + timeout);
            }
            try {
                IqParity.seed(snowflake, session, tables);
                QueryService service = new QueryService();
                int q = 0;
                for (var e : shapes.entrySet()) {
                    String island = IqFuzzTest.island(e.getValue(), profile, tables, names, 0, q++);
                    long planMs = -1;
                    long runMs = -1;
                    long chars = -1;
                    String outcome;
                    String detail;
                    long t0 = System.nanoTime();
                    try {
                        chars = com.legend.Compiler.query(compiled, island).plan(IqParity.RUNTIME).sql().length();
                        planMs = (System.nanoTime() - t0) / 1_000_000;
                        long t1 = System.nanoTime();
                        var result = service.execute(model, island, IqParity.RUNTIME, session);
                        runMs = (System.nanoTime() - t1) / 1_000_000;
                        outcome = "ROWS";
                        detail = result.rows().size() + " rows";
                    } catch (Exception | StackOverflowError x) {
                        IqFuzzTest.Outcome o = IqFuzzTest.classify(x);
                        outcome = o.kind();
                        detail = o.reason();
                        if (planMs >= 0) {
                            runMs = (System.nanoTime() - t0) / 1_000_000 - planMs;
                        }
                        if (outcome.equals("CRASH")) {
                            crashes.add(e.getKey() + ": " + detail);
                        }
                    }
                    String line = e.getKey() + '\t' + outcome + '\t' + planMs + '\t' + chars + '\t' + runMs + '\t'
                            + (detail.length() > 300 ? detail.substring(0, 300) : detail).replace('\n', ' ');
                    System.out.println("[iq-scale-snowflake] " + line);
                    report.append(line).append('\n');
                }
            } finally {
                try (Statement s = session.createStatement()) {
                    s.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
                }
            }
        }
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "iq-scale-snowflake.tsv"), report.toString(), StandardCharsets.UTF_8);
        }
        assertTrue(crashes.isEmpty(), crashes.size() + " crashes:\n" + String.join("\n", crashes));
    }
}
