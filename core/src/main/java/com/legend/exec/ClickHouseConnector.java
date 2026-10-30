// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;


/** ClickHouse's sessions: the platform opens no declared connection to it yet (its sessions are the test harnesses');
 *  a session handed to execution is checked against its driver's product name. */
final class ClickHouseConnector implements DatabaseConnector {

    static final ClickHouseConnector INSTANCE = new ClickHouseConnector();

    private ClickHouseConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.ClickHouse;
    }

    /** The ClickHouse JDBC driver (measured, clickhouse-jdbc 0.10.0 on 25.8). */
    @Override
    public String jdbcProduct() {
        return "ClickHouse";
    }

    @Override
    public Sessions.Opening opening(ConnectionDefinition def) {
        return DatabaseConnector.notOpened(def);
    }

    /** {@inheritDoc} No: {@code ClickHouse} is a server. */
    @Override
    public boolean inProcess() {
        return false;
    }

    /** {@inheritDoc} None: {@code ClickHouse}'s errors carry their raise. */
    @Override
    public @com.legend.base.Nullable RaiseRecovery raiseRecovery() {
        return null;
    }
}
