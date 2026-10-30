// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;


/** BigQuery's sessions: the platform opens no declared connection to it yet (its sessions are the test harnesses');
 *  a session handed to execution is checked against its driver's product name. */
final class BigQueryConnector implements DatabaseConnector {

    static final BigQueryConnector INSTANCE = new BigQueryConnector();

    private BigQueryConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.BigQuery;
    }

    /** Google's BigQuery JDBC driver (measured, google-cloud-bigquery-jdbc 1.6.0). */
    @Override
    public String jdbcProduct() {
        return "Google BigQuery";
    }

    @Override
    public Sessions.Opening opening(ConnectionDefinition def) {
        return DatabaseConnector.notOpened(def);
    }

    /** {@inheritDoc} No: {@code BigQuery} is a server. */
    @Override
    public boolean inProcess() {
        return false;
    }

    /** {@inheritDoc} None: {@code BigQuery}'s errors carry their raise. */
    @Override
    public @com.legend.base.Nullable RaiseRecovery raiseRecovery() {
        return null;
    }

    /** {@inheritDoc} The dialect's {@code dropsDecimalTrailingZeros} until D72 (2026-10-30), carried: The driver hands an ARRAY as a java.sql.Array and a STRUCT as a java.sql.Struct (measured:
     *  BigQueryJsonArray, BigQueryJsonStruct), DATETIME and TIMESTAMP as java.sql.Timestamp, and NUMERIC and BIGNUMERIC
     *  as a BigDecimal with its trailing zeros dropped ({@code BIGNUMERIC '3.0'} reads 3, measured): the column's
     *  declared scale is the value's own, so the reader pads the digits back, never rounding. */
    @Override
    public DatabaseConnector.Decoding decoding() {
        return new DatabaseConnector.Decoding(true, true, false, false);
    }
}
