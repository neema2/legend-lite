// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;


/** MariaDB's sessions: a server, reached by its coordinates through MariaDB Connector/J, which opens MySQL too
 *  (measured on MariaDB 11.4 and MySQL 8.4: one driver, one URL; {@link MySqlConnector}). */
class MariaDbConnector implements DatabaseConnector {

    static final MariaDbConnector INSTANCE = new MariaDbConnector();

    MariaDbConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.MariaDB;
    }

    /** MariaDB Connector/J names the server it reached (measured, 3.5.10 on MariaDB 11.4). */
    @Override
    public String jdbcProduct() {
        return "MariaDB";
    }

    @Override
    public Sessions.Opening opening(ConnectionDefinition def) {
        if (def.specification() instanceof ConnectionSpecification.StaticDatasource(String host, int port,
                String database)) {
            return new Sessions.Opening.Url("jdbc:mariadb://" + host + ":" + port + "/" + database);
        }
        return DatabaseConnector.refused(def);
    }

    /** {@inheritDoc} No: MariaDB is a server. */
    @Override
    public boolean inProcess() {
        return false;
    }

    /** {@inheritDoc} A MariaDB statement cannot raise an error of its own text, so the dialect keeps the message in the
     *  session's user variable {@code @legend_raise} before it fails ({@code sql.dialect.MariaDb}'s raise, and
     *  MySQL's): one fixed read of the session's state, and its reset, never a query of the user's. */
    @Override
    public RaiseRecovery raiseRecovery() {
        return new RaiseRecovery("SELECT @legend_raise", "SET @legend_raise = NULL");
    }

    @Override
    public String storelessDeclaration() {
        throw new IllegalArgumentException(type() + " is a server: its storeless runtime declares the session's"
                + " coordinates (onServer)");
    }
}
