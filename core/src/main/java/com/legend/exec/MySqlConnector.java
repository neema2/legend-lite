// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.DatabaseType;

/** MySQL's sessions: MariaDB's ({@link MariaDbConnector}), the server named by the driver as MySQL. */
final class MySqlConnector extends MariaDbConnector {

    static final MySqlConnector INSTANCE = new MySqlConnector();

    private MySqlConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.MySQL;
    }

    /** MariaDB Connector/J names the server it reached (measured, 3.5.10 on MySQL 8.4). */
    @Override
    public String jdbcProduct() {
        return "MySQL";
    }
}
