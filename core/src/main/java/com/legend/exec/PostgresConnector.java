// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ConnectionSpecification;


/** Postgres's sessions: a server, reached by its coordinates. */
final class PostgresConnector implements DatabaseConnector {

    static final PostgresConnector INSTANCE = new PostgresConnector();

    private PostgresConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.Postgres;
    }

    @Override
    public String jdbcProduct() {
        return "PostgreSQL";
    }

    @Override
    public Sessions.Opening opening(com.legend.executionplan.Connection def) {
        if (def.specification() instanceof ConnectionSpecification.StaticDatasource(String host, int port,
                String database)) {
            return new Sessions.Opening.Url("jdbc:postgresql://" + host + ":" + port + "/" + database);
        }
        return DatabaseConnector.refused(def);
    }

    @Override
    public String storelessDeclaration() {
        throw new IllegalArgumentException(type() + " is a server: its storeless runtime declares the session's"
                + " coordinates (onServer)");
    }

    /** {@inheritDoc} No: {@code Postgres} is a server. */
    @Override
    public boolean inProcess() {
        return false;
    }

    /** {@inheritDoc} Yes: Postgres aborts the transaction on a failed statement ("current transaction is aborted, commands ignored until end of transaction block"). */
    @Override
    public boolean abortsTransactionOnError() {
        return true;
    }

    /** {@inheritDoc} Yes: its driver takes autocommit off, a commit and a rollback. */
    @Override
    public boolean takesTransactions() {
        return true;
    }
}
