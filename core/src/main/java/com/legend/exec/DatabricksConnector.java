// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.DatabaseType;


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
    public Sessions.Opening opening(com.legend.executionplan.Connection def) {
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

    /** {@inheritDoc} No: Databricks holds no transaction the platform opens across statements (none is kept in a turn). */
    @Override
    public boolean abortsTransactionOnError() {
        return false;
    }

    /** {@inheritDoc} The dialect's {@code refetchesTimestampAsJavaTime} until D72 (2026-10-30), carried: No java.time re-fetch (measured 2026-10-11): its driver answers getObject(int, LocalDateTime.class)
     *  with the timestamp as text. A timestamp cell keeps the driver's Timestamp;
     *  a BC date-time is designed, not built (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §15). */
    @Override
    public DatabaseConnector.Decoding decoding() {
        return new DatabaseConnector.Decoding(false, false, false, false);
    }
}
