// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test-only seeding: runs raw SQL on the connection a model's runtime resolves to, the same leased (and, for an
 * in-memory database, cached) connection the server's queries then read. It replaces the product's
 * {@code QueryService.executeSql} and the {@code /engine/sql} route, deleted by execution plan W0.1: raw SQL from a
 * caller is not a product surface.
 */
final class Seed {

    private Seed() {
    }

    static void sql(String model, String sql, String runtimeName) throws SQLException {
        try (ConnectionResolver.Lease lease = ConnectionResolver.lease(com.legend.Compiler.compileModel(model), runtimeName);
                Statement stmt = lease.connection().createStatement()) {
            stmt.execute(sql);
        }
    }
}
