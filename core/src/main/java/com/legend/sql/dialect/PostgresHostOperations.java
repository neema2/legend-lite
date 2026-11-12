// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;

import java.util.List;

/**
 * Postgres's host operations ({@link HostOperations}): the one a host asks of a Postgres database today, a query's rows
 * counted -- Snap's preflight on a warehouse's catalog that is Postgres (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5),
 * written as Postgres reads it, its subquery aliased as Postgres before 16 requires. Every other is refused by name.
 */
final class PostgresHostOperations implements HostOperations {

    static final PostgresHostOperations INSTANCE = new PostgresHostOperations();

    private PostgresHostOperations() {
    }

    @Override
    public List<String> openDatabase(String name) {
        throw new DialectCapability("a project's own database on Postgres is not built: only DuckDB makes one so far "
                + "(docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5)");
    }

    @Override
    public List<String> useDatabase(String name) {
        return openDatabase(name);
    }

    @Override
    public List<String> closeDatabase(String name) {
        return openDatabase(name);
    }

    @Override
    public List<String> statements(HostOperation operation) {
        if (operation instanceof HostOperation.Count c && c.from() instanceof HostOperation.Source.Query q) {
            return List.of("SELECT count(*) AS n FROM (" + q.sql() + ") AS counted");
        }
        throw HostOperations.refusal(operation, "Postgres");
    }

    @Override
    public @com.legend.base.Nullable TransactionStatements transaction() {
        // a host counts on Postgres and runs no plan there: its targets carry no transaction
        return null;
    }
}
