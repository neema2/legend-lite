// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

/**
 * Test-only seeding: runs raw SQL on the connection a model's runtime resolves to, the same leased (and, for an
 * in-memory database, cached) connection the server's queries then read. It replaces the product's
 * {@code QueryService.executeSql} and the {@code /engine/sql} route, deleted by execution plan W0.1: raw SQL from a
 * caller is not a product surface.
 */
final class Seed {

    private Seed() {
    }

    static void sql(String model, String sql, String runtimeName) {
        // the server's session for the runtime, its statements the seam's (D70: no lease, no raw connection)
        try (com.legend.exec.Sessions.Session session =
                ConnectionResolver.session(com.legend.Compiler.compileModel(model), runtimeName)) {
            session.statements().execute(sql);
        }
    }
}
