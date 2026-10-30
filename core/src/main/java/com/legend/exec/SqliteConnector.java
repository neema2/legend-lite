// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** SQLite's sessions: a file or an in-memory database, which lives exactly as long as its one connection
 *  ({@link Sessions.Opening.Held}). */
final class SqliteConnector implements DatabaseConnector {

    static final SqliteConnector INSTANCE = new SqliteConnector();

    private SqliteConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.SQLite;
    }

    @Override
    public String jdbcProduct() {
        return "SQLite";
    }

    @Override
    public Sessions.Opening opening(ConnectionDefinition def) {
        if (def.specification() instanceof ConnectionSpecification.LocalFile(String path)) {
            return new Sessions.Opening.Url("jdbc:sqlite:" + path);
        }
        if (def.specification() instanceof ConnectionSpecification.InMemory) {
            return new Sessions.Opening.Held(this);
        }
        return DatabaseConnector.refused(def);
    }

    @Override
    public boolean inProcess() {
        return true;
    }

    @Override
    public Connection openPrivate() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite::memory:");
    }

    @Override
    public String storelessDeclaration() {
        return "specification: SQLite { };\n  auth: Test";
    }

    /** {@inheritDoc} None: {@code SQLite}'s errors carry their raise. */
    @Override
    public @com.legend.base.Nullable RaiseRecovery raiseRecovery() {
        return null;
    }
}
