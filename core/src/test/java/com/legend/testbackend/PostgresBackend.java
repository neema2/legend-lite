// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testing.EmbeddedPostgres;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/** Postgres's sessions: this JVM's embedded Postgres 16, a new session on its database {@code postgres}. A corpus
 *  workspace is a DATABASE of its own on that server (2026-10-12, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §20: the corpus
 *  stores' declared schemas live inside it undisturbed, as in a DuckDB catalog), created over one administrative
 *  connection and dropped when the workspace's connection closes. */
final class PostgresBackend extends ServerBackend {

    static final PostgresBackend INSTANCE = new PostgresBackend();

    private final AtomicInteger workspaceIds = new AtomicInteger();

    /** The administrative connection (to the server's own {@code postgres} database): workspaces are made and dropped
     *  over it. */
    private @com.legend.base.Nullable Connection admin;

    private PostgresBackend() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.Postgres;
    }

    @Override
    public String name() {
        return "postgres";
    }

    /** A session as the platform opens one, as the launcher's user. */
    @Override
    Connection session(String database) throws SQLException {
        java.util.Properties settings = new java.util.Properties();
        settings.setProperty("user", EmbeddedPostgres.USER);
        return openedAsThePlatformOpens(database, settings);
    }

    @Override
    int port() {
        return EmbeddedPostgres.shared().port();
    }

    @Override
    String database() {
        return "postgres";
    }

    @Override
    public synchronized Connection openWorkspace() throws SQLException {
        if (admin == null) {
            admin = session(database());
        }
        String ws = "ws_" + workspaceIds.getAndIncrement();
        try (Statement st = admin.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("CREATE DATABASE " + ws);
        }
        return closeDrops(session(ws), ws);
    }

    /** The workspace's connection, whose close drops its database too. */
    private Connection closeDrops(Connection inner, String ws) {
        InvocationHandler h = (proxy, method, args) -> {
            if ("close".equals(method.getName())) {
                try {
                    inner.close();
                } finally {
                    drop(ws);
                }
                return null;
            }
            try {
                return method.invoke(inner, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (Connection) Proxy.newProxyInstance(PostgresBackend.class.getClassLoader(),
                new Class<?>[] {Connection.class}, h);
    }

    private synchronized void drop(String ws) {
        try (Statement st = admin.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("DROP DATABASE IF EXISTS " + ws + " WITH (FORCE)");
        } catch (SQLException e) {
            // a failed drop must not mask the test's own outcome; the server is this run's alone
            System.err.println("[corpus2] workspace " + ws + " not dropped: " + e.getMessage());
        }
    }
}
