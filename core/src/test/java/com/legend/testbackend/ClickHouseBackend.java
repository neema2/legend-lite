// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testing.EmbeddedClickHouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** ClickHouse's sessions: this JVM's embedded ClickHouse 25.8, a new session (its driver's HTTP) on {@code default}. */
final class ClickHouseBackend extends ServerBackend {

    static final ClickHouseBackend INSTANCE = new ClickHouseBackend();

    private ClickHouseBackend() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.ClickHouse;
    }

    @Override
    public String name() {
        return "clickhouse";
    }

    /** The launcher's session: the platform opens none on ClickHouse (its connector's opening is
     *  {@code notOpened}), so there is no platform route to hold it to. */
    @Override
    Connection session(String database) throws SQLException {
        return DriverManager.getConnection(EmbeddedClickHouse.shared().jdbcUrl(database));
    }

    @Override
    int port() {
        return EmbeddedClickHouse.shared().port();
    }

    @Override
    String database() {
        return "default";
    }

    /** A session of its own: ClickHouse keeps a temporary table only in an HTTP session. */
    @Override
    Connection system() throws SQLException {
        return DriverManager.getConnection(EmbeddedClickHouse.shared().sessionJdbcUrl(database()));
    }

    /** Numbers each workspace's database. */
    private final java.util.concurrent.atomic.AtomicInteger workspaceIds = new java.util.concurrent.atomic.AtomicInteger();

    /** {@inheritDoc} A database made for it, {@code ws_<n>}, and a session of its own on it (an HTTP session: a
     *  {@code SET} holds for its statements); closing it drops the database with everything in it, then closes the
     *  session (2026-11-07, docs/TS_DECISIONS_2026_11_07.md item 9: the gallery's replay on ClickHouse, a v1
     *  database). */
    @Override
    public Connection openWorkspace() throws SQLException {
        String ws = "ws_" + workspaceIds.getAndIncrement() + "_" + ProcessHandle.current().pid();
        try (Connection admin = session(database()); java.sql.Statement st = admin.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("CREATE DATABASE " + ws);
        }
        Connection session;
        try {
            session = DriverManager.getConnection(EmbeddedClickHouse.shared().sessionJdbcUrl(ws));
        } catch (SQLException e) {
            drop(ws, e);
            throw e;
        }
        return (Connection) java.lang.reflect.Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close") && method.getParameterCount() == 0) {
                        if (!session.isClosed()) {
                            try {
                                session.close();
                            } finally {
                                drop(ws, null);
                            }
                        }
                        return null;
                    }
                    try {
                        return method.invoke(session, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    /** Drops workspace {@code ws}; a failure added to {@code cause} where there is one. */
    private void drop(String ws, @com.legend.base.Nullable SQLException cause) throws SQLException {
        try (Connection admin = session(database()); java.sql.Statement st = admin.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("DROP DATABASE IF EXISTS " + ws);
        } catch (SQLException e) {
            if (cause == null) {
                throw e;
            }
            cause.addSuppressed(e);
        }
    }
}
