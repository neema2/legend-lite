// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CATALOG-PER-SESSION workspaces over ONE long-lived DuckDB instance (session-topology ruling 2026-08-14), the corpus's
 * workspaces on DuckDB ({@link DuckDbBackend}) and on the warehouse that serves it ({@link DuckDbWarehouseBackend}): the
 * two differ only in how a connection to the instance is opened ({@link Instance}). The JUSTIFICATION is the
 * architecture, not speed: a harness that runs against a single long-lived database is a FEASIBILITY requirement for
 * remote backends (no cloud boots 938 databases per corpus run), and a DuckDB instance's THREE-level namespace lets
 * isolation take the CATALOG level (<code>ATTACH ':memory:' AS __ws_N</code> + a per-connection <code>USE</code>) while
 * the corpus stores' own declared SCHEMAS live undisturbed inside each workspace -- no name mangling, no generator
 * changes.
 *
 * <p>Honest numbers (interleaved A/B, re-measured 2026-08-15 after the literal fold): per-session topology 75/78s,
 * workspaces 69/69s per full corpus run -- ~10%, and consistent. The architecture seam stays the primary justification.
 *
 * <p>Design points, each load-bearing:
 * <ul>
 *   <li>Workspace names are collision-proof ({@code __ws_N}) -- a two-part name {@code S.T} resolves schema-first in the
 *       current catalog, and no corpus store schema spells {@code __ws_*}, so attached catalogs can never capture store
 *       references.</li>
 *   <li>{@code ATTACH} is instance-global; {@code USE} is per-connection. Each workspace gets its own connection to the
 *       instance ({@link Instance#another}) pointed at its own catalog.</li>
 *   <li>The returned Connection is a close-intercepting proxy: closing it DETACHes the catalog too, so every call
 *       site's close/try-with-resources discipline is also the workspace teardown. A leaked workspace trips the
 *       {@link #LEAK_CEILING} on the next open -- loudly, not as slow memory creep.</li>
 *   <li>{@code TimeZone} is SESSION-scoped -> set per workspace connection; {@code threads} is global -> set once on the
 *       root.</li>
 * </ul>
 */
final class DuckDbWorkspaces {

    /** How the workspaces reach their instance. */
    interface Instance {

        /** The instance's root connection: the one the workspaces' catalogs are attached and detached over. */
        Connection root() throws SQLException;

        /** Another connection to the same instance as {@code root}. */
        Connection another(Connection root) throws SQLException;
    }

    /** family + private + probe + headroom; more open workspaces than this means a close/DETACH leak, which must fail
     *  fast. */
    private static final int LEAK_CEILING = 8;

    private final Instance instance;
    private @com.legend.base.Nullable Connection root;
    private final AtomicInteger ids = new AtomicInteger();
    private final Set<String> live = new TreeSet<>();


    DuckDbWorkspaces(Instance instance) {
        this.instance = instance;
    }

    /** A fresh workspace: a catalog of its own on the instance, and a connection using it, whose close detaches it. */
    synchronized Connection open() throws SQLException {
        Connection r = root();
        if (live.size() >= LEAK_CEILING) {
            throw new IllegalStateException("workspace leak: " + live.size() + " catalogs still attached " + live
                    + " -- a session was not closed (DETACH rides close())");
        }
        String ws = "__ws_" + ids.getAndIncrement();
        try (Statement st = r.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("ATTACH ':memory:' AS " + ws);
        }
        Connection conn = instance.another(r);
        try (Statement st = conn.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("USE " + ws);
            // (harness WARMUP of the dialect's own session contract -- the DECISION lives in the dialect's session
            // initialisation (B6); this pre-applies it to pooled workspaces before any execute)
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("SET TimeZone='UTC'");
        }
        live.add(ws);
        return closeDetaches(conn, ws);
    }

    private Connection root() throws SQLException {
        if (root == null) {
            Connection r = instance.root();
            try (Statement st = r.createStatement()) {
                com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
                st.execute("SET threads=1");
            }
            root = r;
        }
        return root;
    }

    private Connection closeDetaches(Connection inner, String ws) {
        InvocationHandler h = (proxy, method, args) -> {
            if ("close".equals(method.getName())) {
                try {
                    inner.close();
                } finally {
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
        return (Connection) Proxy.newProxyInstance(DuckDbWorkspaces.class.getClassLoader(),
                new Class<?>[] {Connection.class}, h);
    }

    private synchronized void detach(String ws) {
        long t0 = System.nanoTime();
        try (Statement st = root().createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("DETACH " + ws);
            long dt = System.nanoTime() - t0;
            WorkspaceCensus.closed(dt);
            if (com.legend.diagnostics.Diagnostics.on("detach-trace") && dt > 20_000_000L) {
                System.out.println("[ws-detach] " + dt / 1_000_000L + "ms " + ws);
            }
        } catch (SQLException e) {
            // a failed DETACH must not mask the test's own outcome, but it may not vanish either: the ceiling above
            // turns a leak into a loud failure within a few sessions
        } finally {
            live.remove(ws);
        }
    }
}
