// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ConnectionSpecification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** DuckDB's sessions: a file or an in-memory database, which lives exactly as long as its one connection
 *  ({@link Sessions.Opening.Held}). */
final class DuckDbConnector implements DatabaseConnector {

    static final DuckDbConnector INSTANCE = new DuckDbConnector();

    private DuckDbConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.DuckDB;
    }

    @Override
    public String jdbcProduct() {
        return "DuckDB";
    }

    @Override
    public Sessions.Opening opening(com.legend.executionplan.Connection def) {
        if (def.specification() instanceof ConnectionSpecification.LocalFile(String path)) {
            return new Sessions.Opening.Url("jdbc:duckdb:" + path);
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
    public Connection openPrivate(com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings) throws SQLException {
        DatabaseConnector.noSettings(type(), settings);
        return DriverManager.getConnection("jdbc:duckdb:");
    }

    @Override
    public String storelessDeclaration() {
        return "specification: DuckDB { };\n  auth: Test";
    }

    /** {@inheritDoc} Yes: DuckDB aborts the whole transaction on a failed statement and refuses every statement after it until a rollback (measured: "Current transaction is aborted (please ROLLBACK)"). */
    @Override
    public boolean abortsTransactionOnError() {
        return true;
    }

    /** {@inheritDoc} Yes: its driver takes autocommit off, a commit and a rollback. */
    @Override
    public boolean takesTransactions() {
        return true;
    }

    /** {@inheritDoc} The dialect's {@code jsonSpellsNonFiniteNumbers} until D72 (2026-10-30), carried: Yes: DuckDB's JSON writes a double that is not finite as {@code NaN}, {@code Infinity} or
     *  {@code -Infinity} (a float divided by a zero inside a list; the Cypher TCK's ordered values of every type). */
    @Override
    public DatabaseConnector.Decoding decoding() {
        return new DatabaseConnector.Decoding(true, false, false, true);
    }
}
