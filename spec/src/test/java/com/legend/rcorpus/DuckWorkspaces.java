// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import org.duckdb.DuckDBConnection;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CATALOG-PER-SESSION workspaces over ONE long-lived DuckDB instance
 * (session-topology ruling 2026-08-14). The JUSTIFICATION is the
 * architecture, not speed: a harness that runs against a single
 * long-lived database is a FEASIBILITY requirement for future remote
 * backends (Snowflake/Postgres cannot boot 938 databases per corpus
 * run), and a DuckDB instance's THREE-level namespace lets isolation
 * take the CATALOG level (<code>ATTACH ':memory:' AS __ws_N</code> + a
 * per-connection <code>USE</code>) while the corpus stores' own
 * declared SCHEMAS live undisturbed inside each workspace — no name
 * mangling, no generator changes. The same shape is Snowflake's native
 * idiom (<code>CREATE DATABASE w CLONE fam</code>); H2 keeps
 * instance-per-session (its instances ARE cheap catalogs).
 *
 * <p>Honest numbers (interleaved A/B, re-measured 2026-08-15 after the
 * literal fold): per-session topology 75/78s, workspaces 69/69s per
 * full DuckDB corpus run — ~10%, and consistent. (The 2026-08-14
 * pre-fold measurement said ~6%; with the literal round trips gone the
 * boot+cold-catalog share GREW. The original cold-process
 * microbenchmark's ~18s forecast remains wrong — warm in-run boots are
 * far cheaper.) The architecture seam stays the primary justification.
 *
 * <p>(Direct {@code org.duckdb} import is fine HERE: ArchitectureTest's
 * driver ban covers {@code src/main} only — production stays
 * JDBC-generic, the harness speaks the backend's native idiom.)
 *
 * <p>Design points, each load-bearing:
 * <ul>
 *   <li>Workspace names are collision-proof ({@code __ws_N}) — a
 *       two-part name {@code S.T} resolves schema-first in the current
 *       catalog, and no corpus store schema spells {@code __ws_*}, so
 *       attached catalogs can never capture store references.</li>
 *   <li>{@code ATTACH} is instance-global; {@code USE} is
 *       per-connection. Each workspace gets its own
 *       {@link DuckDBConnection#duplicate()} pointed at its own
 *       catalog.</li>
 *   <li>The returned Connection is a close-intercepting proxy: closing
 *       it DETACHes the catalog too, so every existing call site's
 *       close/try-with-resources discipline is also the workspace
 *       teardown. A leaked workspace trips the {@link #LEAK_CEILING}
 *       on the next open — loudly, not as slow memory creep.</li>
 *   <li>{@code TimeZone} is SESSION-scoped -> set per workspace
 *       connection; {@code threads} is global -> set once on the
 *       root.</li>
 * </ul>
 */
final class DuckWorkspaces {

    /** family + private + probe + headroom; more open workspaces than
     *  this means a close/DETACH leak, which must fail fast. */
    private static final int LEAK_CEILING = 8;

    /** The warehouse's deploy jar: set, every connection is a session on a
     *  warehouse this harness starts (W1c, docs/WAREHOUSE_W1_DESIGN_2026_09_26.md);
     *  unset, the in-process DuckDB below. */
    private static final @com.legend.Nullable String WAREHOUSE_JAR =
            System.getProperty("rcorpus.warehouse.server");

    /** The instance's root connection: in process, the DuckDB connection the
     *  others duplicate; on a warehouse, a session of its own. */
    private static Connection root;
    private static @com.legend.Nullable String warehouseUrl;
    private static final AtomicInteger IDS = new AtomicInteger();
    private static final Set<String> LIVE = new TreeSet<>();

    private DuckWorkspaces() {
    }

    static synchronized Connection open() throws SQLException {
        if (root == null) {
            if (WAREHOUSE_JAR != null) {
                warehouseUrl = startWarehouse(WAREHOUSE_JAR);
                root = DriverManager.getConnection(warehouseUrl);
            } else {
                root = DriverManager.getConnection("jdbc:duckdb:");
            }
            try (Statement st = root.createStatement()) {
                com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
                st.execute("SET threads=1");
            }
        }
        if (LIVE.size() >= LEAK_CEILING) {
            throw new IllegalStateException(
                    "workspace leak: " + LIVE.size() + " catalogs still"
                            + " attached " + LIVE + " — a session was not"
                            + " closed (DETACH rides close())");
        }
        String ws = "__ws_" + IDS.getAndIncrement();
        try (Statement st = root.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("ATTACH ':memory:' AS " + ws);
        }
        Connection conn = another();
        try (Statement st = conn.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("USE " + ws);
            // (harness WARMUP of the dialect's own session contract —
            // the DECISION lives in DuckDb.initSession (B6); this
            // pre-applies it to pooled workspaces before any execute)
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("SET TimeZone='UTC'");
        }
        LIVE.add(ws);
        return closeDetaches(conn, ws);
    }

    /** Another connection to the same instance: in process, a duplicate of
     *  the root; on a warehouse, a session of its own on the same catalog. */
    private static Connection another() throws SQLException {
        String url = warehouseUrl;
        return url != null ? DriverManager.getConnection(url)
                : ((DuckDBConnection) root).duplicate();
    }

    /** Starts the warehouse as a child process on a free port, with an empty
     *  data directory and one user; stopped when this JVM exits. Its own
     *  classpath: the warehouse runs DuckDB 1.5.5.1, this harness 1.4.4. */
    private static String startWarehouse(String jar) throws SQLException {
        try {
            java.nio.file.Path data = java.nio.file.Files.createTempDirectory("rcorpus-warehouse");
            String launcher = java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString();
            Process p = new ProcessBuilder(launcher, "--enable-native-access=ALL-UNNAMED", "-jar", jar,
                    "--port", "0", "--data", data.toString(), "--user", "rcorpus:rcorpus",
                    "--concurrency", "4")
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .start();
            Runtime.getRuntime().addShutdownHook(new Thread(p::destroy));
            java.io.BufferedReader err = new java.io.BufferedReader(
                    new java.io.InputStreamReader(p.getErrorStream(), java.nio.charset.StandardCharsets.UTF_8));
            java.util.regex.Pattern ready = java.util.regex.Pattern.compile("warehouse listening on 127\\.0\\.0\\.1:(\\d+)");
            String line;
            while ((line = err.readLine()) != null) {
                System.err.println("[warehouse] " + line);
                java.util.regex.Matcher m = ready.matcher(line);
                if (m.find()) {
                    Thread drain = new Thread(() -> {
                        try {
                            String l;
                            while ((l = err.readLine()) != null) System.err.println("[warehouse] " + l);
                        } catch (java.io.IOException ignored) {
                            // the process ended
                        }
                    }, "warehouse-stderr");
                    drain.setDaemon(true);
                    drain.start();
                    return "jdbc:warehouse:http://127.0.0.1:" + m.group(1)
                            + "/main?user=rcorpus&password=rcorpus";
                }
            }
            throw new SQLException("the warehouse exited before listening (exit " + p.waitFor() + ")");
        } catch (java.io.IOException e) {
            throw new SQLException("cannot start the warehouse from " + jar, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted starting the warehouse", e);
        }
    }

    /** The ASIDE catalogs a workspace attached for clashing fixtures
     * (TestObserver.isolateFixture), by the proxied connection: names
     * stay as written; the workspace's own tables resolve first, the
     * asides last, once the fixture has run. */
    private static final java.util.Map<Connection, java.util.List<String>> ASIDES =
            new java.util.IdentityHashMap<>();
    private static final java.util.Map<Connection, String> WS_OF =
            new java.util.IdentityHashMap<>();

    /** Attach a fresh aside catalog and make it the CURRENT target: the
     * fixture's unqualified DDL and inserts land there. */
    static synchronized @com.legend.Nullable String isolateBegin(Connection proxied) throws SQLException {
        String ws = WS_OF.get(proxied);
        if (ws == null) {
            return null;   // not a DuckDB workspace (the H2 lane): no primitive
        }
        String aside = ws + "_aside_" + ASIDES.getOrDefault(proxied, java.util.List.of()).size();
        // ATTACH is instance-global and refuses to run inside an open
        // transaction (a body's attempt may be one): it rides the ROOT,
        // like the workspace attach
        try (Statement st = root.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("ATTACH ':memory:' AS " + aside);
        }
        ASIDES.computeIfAbsent(proxied, c -> new java.util.ArrayList<>()).add(aside);
        return aside;
    }

    /** The connection an aside fixture runs on: its OWN (a DuckDB
     * transaction writes to one attached database only, and a body's
     * attempt may hold the session's), pointed at the aside — unqualified
     * DDL, drops and inserts see the aside alone, never the session's
     * same-named tables. Closed by {@link #isolateEnd}. */
    static synchronized Connection asideConnection(String aside) throws SQLException {
        Connection c = another();
        try (Statement st = c.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("USE " + aside);
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("SET TimeZone='UTC'");
        }
        return c;
    }

    /** The asides a workspace holds, in attach order. */
    static synchronized java.util.List<String> asidesOf(Connection proxied) {
        return java.util.List.copyOf(ASIDES.getOrDefault(proxied, java.util.List.of()));
    }

    /** The workspace first, every aside after it, in attach order. */
    static synchronized void isolateEnd(Connection proxied) throws SQLException {
        String ws = WS_OF.get(proxied);
        java.util.List<String> path = new java.util.ArrayList<>();
        path.add(ws + ".main");
        for (String a : ASIDES.getOrDefault(proxied, java.util.List.of())) {
            path.add(a + ".main");
        }
        try (Statement st = proxied.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("SET search_path = '" + String.join(",", path) + "'");
        }
    }

    private static Connection closeDetaches(Connection inner, String ws) {
        Connection[] self = new Connection[1];
        InvocationHandler h = (proxy, method, args) -> {
            if ("close".equals(method.getName())) {
                try {
                    inner.close();
                } finally {
                    java.util.List<String> asides;
                    synchronized (DuckWorkspaces.class) {
                        asides = ASIDES.remove(self[0]);
                        WS_OF.remove(self[0]);
                    }
                    if (asides != null) {
                        for (String a : asides) {
                            detach(a);
                        }
                    }
                    detach(ws);
                }
                return null;
            }
            try {
                return method.invoke(inner, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        };
        Connection proxied = (Connection) Proxy.newProxyInstance(
                DuckWorkspaces.class.getClassLoader(),
                new Class<?>[] {Connection.class}, h);
        self[0] = proxied;
        synchronized (DuckWorkspaces.class) {
            WS_OF.put(proxied, ws);
        }
        return proxied;
    }

    /** DETACH timing census (perf homework 2026-09-20): count, total ms, max ms. */
    static final java.util.concurrent.atomic.AtomicLong DETACHES = new java.util.concurrent.atomic.AtomicLong();
    static final java.util.concurrent.atomic.AtomicLong DETACH_NANOS = new java.util.concurrent.atomic.AtomicLong();
    static final java.util.concurrent.atomic.AtomicLong DETACH_MAX_NANOS = new java.util.concurrent.atomic.AtomicLong();

    private static synchronized void detach(String ws) {
        long t0 = System.nanoTime();
        try (Statement st = root.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("DETACH " + ws);
            long dt = System.nanoTime() - t0;
            DETACHES.incrementAndGet();
            DETACH_NANOS.addAndGet(dt);
            DETACH_MAX_NANOS.accumulateAndGet(dt, Math::max);
            if (System.getProperty("rcorpus.detachTrace") != null && dt > 20_000_000L) {
                System.out.println("[ws-detach] " + dt / 1_000_000L + "ms " + ws);
            }
        } catch (SQLException e) {
            // a failed DETACH must not mask the test's own outcome, but
            // it may not vanish either: the ceiling above turns a leak
            // into a loud failure within a few sessions
        } finally {
            LIVE.remove(ws);
        }
    }
}
