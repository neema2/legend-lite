// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;


/** Snowflake's sessions: the platform opens no declared connection to it yet (its sessions are the test harnesses');
 *  a session handed to execution is checked against its driver's product name. */
final class SnowflakeConnector implements DatabaseConnector {

    static final SnowflakeConnector INSTANCE = new SnowflakeConnector();

    private SnowflakeConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.Snowflake;
    }

    @Override
    public String jdbcProduct() {
        return "Snowflake";
    }

    @Override
    public Sessions.Opening opening(ConnectionDefinition def) {
        return DatabaseConnector.notOpened(def);
    }

    /** {@inheritDoc} No: {@code Snowflake} is a server. */
    @Override
    public boolean inProcess() {
        return false;
    }

    /** {@inheritDoc} None: {@code Snowflake}'s errors carry their raise. */
    @Override
    public @com.legend.base.Nullable RaiseRecovery raiseRecovery() {
        return null;
    }

    /** {@inheritDoc} The dialect's {@code refetchesTimestampAsJavaTime} until D72 (2026-10-30), carried: No java.time re-fetch (measured 2026-10-11): its driver refuses getObject(int, LocalDateTime.class),
     *  "Type passed to getObject(int, Class) is unsupported". A timestamp cell keeps the driver's Timestamp;
     *  a BC date-time is designed, not built (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §15). */
    @Override
    public DatabaseConnector.Decoding decoding() {
        return new DatabaseConnector.Decoding(false, false, false, false);
    }
}
