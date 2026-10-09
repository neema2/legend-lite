// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** The engines' own bulk loaders, found beside their drivers ({@code ServiceLoader}: core compiles against none). */
final class BulkLoads {

    private BulkLoads() {
    }

    private static final List<BulkLoad> LOADERS = java.util.ServiceLoader.load(BulkLoad.class)
            .stream().map(java.util.ServiceLoader.Provider::get).toList();

    /** The loader of {@code connection}'s engine, or null when it has none (it takes one INSERT instead). */
    static @com.legend.base.Nullable BulkLoad of(Connection connection) throws SQLException {
        for (BulkLoad bulk : LOADERS) {
            if (bulk.accepts(connection)) {
                return bulk;
            }
        }
        return null;
    }

    /** Loads a plan's rows step on {@code connection}, through its engine's loader: a plan writes rows for one only
     *  where its database has one ({@code Databases.loadsRowsInBulk}), so none is a mismatch, refused by name. */
    static void load(Connection connection, ExecutionPlan.SetupStep.Rows rows) throws SQLException {
        BulkLoad bulk = of(connection);
        if (bulk == null) {
            throw new IllegalStateException("rows for " + rows.stagingTable() + " are for a bulk loader, and the session's"
                    + " database has none (" + connection.getMetaData().getDatabaseProductName() + ")");
        }
        Census.inc(Census.Key.SQL_ROUND_TRIPS);
        Census.inc(Census.Key.BULK_LOADS);
        StatementOrigin.count();
        bulk.load(connection, rows);
    }
}
