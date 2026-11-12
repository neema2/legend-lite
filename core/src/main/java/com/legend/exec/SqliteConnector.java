// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ConnectionSpecification;

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
    public Sessions.Opening opening(com.legend.executionplan.Connection def) {
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

    /** {@inheritDoc} No: SQLite rolls back the failed statement alone; its transaction goes on. */
    @Override
    public boolean abortsTransactionOnError() {
        return false;
    }

    /** {@inheritDoc} Yes: its driver takes autocommit off, a commit and a rollback. */
    @Override
    public boolean takesTransactions() {
        return true;
    }
}
