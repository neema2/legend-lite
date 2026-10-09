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

    /** Loads a plan's rows step on {@code connection} through {@code bulk}, its engine's loader ({@link #of}). */
    static void load(BulkLoad bulk, Connection connection, ExecutionPlan.SetupStep.Rows rows) throws SQLException {
        Census.inc(Census.Key.SQL_ROUND_TRIPS);
        Census.inc(Census.Key.BULK_LOADS);
        StatementOrigin.count();
        bulk.load(connection, rows);
    }
}
