// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.model.ConnectionDefinition.DatabaseType;

import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/** DuckDB's sessions: a private in-memory instance each, the platform's own engine ({@code Databases.PLATFORM}). Its
 *  corpus workspaces are catalogs of one long-lived in-process instance ({@link DuckDbWorkspaces}). */
final class DuckDbBackend extends InProcessBackend {

    static final DuckDbBackend INSTANCE = new DuckDbBackend();

    /** The workspaces' instance: its root a connection of its own, every other connection a duplicate of it. */
    private final DuckDbWorkspaces workspaces = new DuckDbWorkspaces(new DuckDbWorkspaces.Instance() {
        @Override
        public Connection root() throws SQLException {
            return DriverManager.getConnection("jdbc:duckdb:");
        }

        @Override
        public Connection another(Connection root) throws SQLException {
            return ((DuckDBConnection) root).duplicate();
        }
    });

    private DuckDbBackend() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.DuckDB;
    }

    @Override
    public String name() {
        return "duckdb";
    }

    /** The platform's session in UTC, so TIMESTAMPTZ values come back in UTC (DuckDB's own syntax). */
    @Override
    public java.util.List<String> sessionSetUp() {
        return java.util.List.of("SET timezone='UTC'");
    }

    @Override
    public Connection openWorkspace() throws SQLException {
        return workspaces.open();
    }

    @Override
    public FixtureAside setFixtureAside(Connection workspace) throws SQLException {
        return workspaces.setAside(workspace, name());
    }

    /** A DUPLICATE of the session, pointed back at its workspace's catalog (a duplicate starts outside it; reading the
     *  catalog is metadata, safe while the session holds an open streaming result). */
    @Override
    public Connection readerOn(Connection session) throws SQLException {
        String ws = session.getCatalog();
        if (ws == null || ws.isBlank()) {
            throw new SQLException("a DuckDB session that names no catalog: no reader can be pointed at its data");
        }
        Connection reader = session.unwrap(DuckDBConnection.class).duplicate();
        try (Statement st = reader.createStatement()) {
            com.legend.exec.StatementOrigin.count(com.legend.exec.StatementOrigin.SESSION);
            st.execute("USE " + ws);
        } catch (SQLException e) {
            reader.close();
            throw e;
        }
        return reader;
    }
}
