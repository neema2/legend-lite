// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.testing.Lean;

import com.legend.Compiler;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The SQL stress corpus's runner (core/src/test/resources/sql-stress): each query raw on Postgres, its answer; then,
 * per lane (DuckDB, Postgres), the island PLANNED (typed, lowered, rendered: the time and the SQL's size) and, when
 * the plan is sane, RUN and judged by the parity rules ({@link SqlParity#outcome}). One TSV line per query on standard
 * out. Not a test: a measurement, run by {@code sql-stress/run.sh} one query per JVM, so that a query that never ends
 * is killed with its JVM.
 * Named for its referee, the live Postgres whose answers and fixture ({@link PostgresSeed}) it judges by (SqlStress until
 * 2026-10-17, the dialect program's Phase 6).
 *
 * <p>usage: {@code SqlStressOnPostgres <parity schema dir> <queries.tsv> [id regex]}; {@code -Dstress.pg.url} names the
 * Postgres 16 that answers (required: {@code run.sh} passes a local one on port 55432 by default), {@code
 * -Dstress.timeout} the seconds each plan and each run may take (default 60). The island's other lane is the
 * platform's own engine ({@code TestBackends.platform()}).
 */
public final class SqlStressOnPostgres {

    private static final String PG_URL = java.util.Objects.requireNonNull(System.getProperty("stress.pg.url"),
            "-Dstress.pg.url: the Postgres 16 that answers (run.sh passes one)");
    private static final long TIMEOUT_MS = Long.getLong("stress.timeout", 60) * 1000;
    /** Generated SQL longer than this is not sent to a database: its size is the finding. */
    private static final int MAX_SQL = 2_000_000;

    private SqlStressOnPostgres() {
    }

    public static void main(String[] args) throws Exception {
        var tables = SqlParity.schema(Path.of(args[0]));
        Set<String> names = SqlParity.tableNames(tables);
        List<String[]> queries = Files.readAllLines(Path.of(args[1])).stream().filter(l -> !l.isBlank())
                .map(l -> l.split("\t", 2)).toList();
        String only = args.length > 2 ? args[2] : null;
        // the answering server's host and port, read from its URL (its scheme follows "jdbc:")
        java.net.URI server = java.net.URI.create(PG_URL.substring(PG_URL.indexOf(':') + 1));
        String pgHost = server.getHost();
        String pgPort = String.valueOf(server.getPort());
        TestBackend platform = TestBackends.platform();
        String duckModel = SqlParity.model(tables, platform.connectionBody());
        String pgModel = SqlParity.model(tables, "type: Postgres;\n specification: Static { host: '" + pgHost
                + "'; port: " + pgPort + "; name: 'postgres'; };\n auth: Test;");
        var duckCtx = Compiler.compileModel(duckModel);
        var pgCtx = Compiler.compileModel(pgModel);
        try (Connection seed = DriverManager.getConnection(PG_URL)) {
            PostgresSeed.seed(seed, tables);
        }
        try (Connection duck = platform.open()) {
            SqlParity.seed(platform, duck, tables);
            System.out.println("id\tchars\tpostgres\tpg_ms\tduck_plan_ms\tduck_sql\t" + platform.name()
                    + "\tduck_ms\tpg_plan_ms\tpg_sql\tpostgres_lane\tpgl_ms\tdetail");
            for (String[] q : queries) {
                if (only != null && !q[0].matches(only)) {
                    continue;
                }
                System.out.println(measure(q[0], q[1], names, duck, duckModel, duckCtx, pgModel, pgCtx));
                System.out.flush();
            }
        }
        System.exit(0);
    }

    private static String measure(String id, String sql, Set<String> names, Connection duck, String duckModel,
            com.legend.compiler.element.ModelContext duckCtx, String pgModel, com.legend.compiler.element.ModelContext pgCtx)
            throws Exception {
        SqlParity.Case c = new SqlParity.Case("stress", id, sql, null);
        String island = "#SQL{" + SqlParity.rewrite(sql, names) + "}#";
        long t0 = System.nanoTime();
        SqlParity.Answer answer;
        try (Connection pg = answering()) {
            answer = answer(pg, sql);
        }
        long pgMs = (System.nanoTime() - t0) / 1_000_000;
        String pgSays = answer instanceof SqlParity.Rows r ? r.grid().rows().size() + " rows"
                : "ERR " + ((SqlParity.Rejected) answer).message();
        StringBuilder line = new StringBuilder(id + "\t" + sql.length() + "\t" + pgSays + "\t" + pgMs);
        StringBuilder detail = new StringBuilder();
        for (String lane : List.of("duck", "pg")) {
            var ctx = lane.equals("duck") ? duckCtx : pgCtx;
            AtomicReference<Object> planned = new AtomicReference<>();
            long tp = System.nanoTime();
            boolean finished = within(() -> {
                try {
                    planned.set(Compiler.query(ctx, island).plan(SqlParity.RUNTIME).sql());
                } catch (Throwable e) {
                    planned.set(e);
                }
            });
            long planMs = (System.nanoTime() - tp) / 1_000_000;
            Object p = planned.get();
            if (!finished || p == null) {
                line.append('\t').append(planMs).append("\t-\tPLAN_TIMEOUT\t-");
                continue;
            }
            if (p instanceof Throwable e) {
                String status = answer instanceof SqlParity.Rejected ? "BOTH_ERROR"
                        : e instanceof com.legend.error.NotImplementedException ? "REFUSED" : "PLAN_ERROR";
                line.append('\t').append(planMs).append("\t-\t").append(status).append("\t-");
                detail.append(lane).append(": ").append(oneLine(e.getClass().getSimpleName() + ": "
                        + e.getMessage())).append("  ");
                continue;
            }
            int sqlLength = ((String) p).length();
            line.append('\t').append(planMs).append('\t').append(sqlLength);
            if (sqlLength > MAX_SQL) {
                line.append("\tSQL_TOO_BIG\t-");
                continue;
            }
            Connection connection = lane.equals("duck") ? duck : answering();
            String model = lane.equals("duck") ? duckModel : pgModel;
            AtomicReference<SqlParity.Outcome> out = new AtomicReference<>();
            long t1 = System.nanoTime();
            within(() -> {
                try {
                    out.set(SqlParity.outcome(c, answer, names, isl -> Lean.run(model, isl,
                            SqlParity.RUNTIME, connection)));
                } catch (Throwable e) {
                    out.set(new SqlParity.Outcome(SqlParity.Status.ERROR, e.getClass().getSimpleName() + ": "
                            + e.getMessage()));
                }
            });
            long ms = (System.nanoTime() - t1) / 1_000_000;
            SqlParity.Outcome o = out.get();
            if (o == null) {
                o = new SqlParity.Outcome(SqlParity.Status.ERROR, "RUN_TIMEOUT");
            }
            if (connection != duck) {
                connection.close();
            }
            line.append('\t').append(o.status()).append('\t').append(ms);
            if (!o.detail().isEmpty()) {
                detail.append(lane).append(": ").append(oneLine(o.detail())).append("  ");
            }
        }
        return line + "\t" + detail;
    }

    /** Runs {@code work} on a thread with the JVM's default stack (as a server's), waiting at most the timeout. */
    private static boolean within(Runnable work) throws InterruptedException {
        Thread t = new Thread(null, work, "stress", 0);
        t.setDaemon(true);
        t.start();
        t.join(TIMEOUT_MS);
        return !t.isAlive();
    }

    private static Connection answering() throws SQLException {
        Connection c = DriverManager.getConnection(PG_URL);
        try (Statement s = c.createStatement()) {
            s.execute("SET statement_timeout = '" + TIMEOUT_MS / 1000 + "s'");
        }
        return c;
    }

    private static SqlParity.Answer answer(Connection pg, String sql) {
        try (Statement s = pg.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return new SqlParity.Rows(SqlParity.grid(rs));
        } catch (SQLException e) {
            String message = e.getMessage() == null ? "" : e.getMessage().split("\n")[0];
            return new SqlParity.Rejected(e.getSQLState() == null ? "" : e.getSQLState(), message);
        }
    }

    private static String oneLine(String s) {
        String m = s.replace('\n', ' ').replace('\t', ' ');
        return m.length() > 200 ? m.substring(0, 200) : m;
    }
}
