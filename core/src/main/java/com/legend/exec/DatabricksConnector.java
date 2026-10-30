// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;

import java.sql.Connection;

/** Databricks's sessions: the platform opens no declared connection to it yet (its sessions are the test harnesses');
 *  a session handed to execution is checked against its driver's product name. */
final class DatabricksConnector implements DatabaseConnector {

    static final DatabricksConnector INSTANCE = new DatabricksConnector();

    private DatabricksConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.Databricks;
    }

    /** The Databricks JDBC driver reports Spark's name (measured, databricks-jdbc 3.4.3). */
    @Override
    public String jdbcProduct() {
        return "SparkSQL";
    }

    @Override
    public Sessions.Opening opening(ConnectionDefinition def) {
        return DatabaseConnector.notOpened(def);
    }

    /** {@inheritDoc} No: {@code Databricks} is a server. */
    @Override
    public boolean inProcess() {
        return false;
    }

    /** {@inheritDoc} None: {@code Databricks}'s errors carry their raise. */
    @Override
    public @com.legend.base.Nullable RaiseRecovery raiseRecovery() {
        return null;
    }
}
