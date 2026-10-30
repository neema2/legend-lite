// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.model.ConnectionDefinition.DatabaseType;

/** SQLite's sessions: a private in-memory instance each (the driver's own build, in-process). */
final class SqliteBackend extends InProcessBackend {

    static final SqliteBackend INSTANCE = new SqliteBackend();

    private SqliteBackend() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.SQLite;
    }

    @Override
    public String name() {
        return "sqlite";
    }

    /** SQLite has no {@code EXTRACT}: its {@code strftime} over {@code now}. */
    @Override
    public String yearOfNowSql() {
        return "SELECT strftime('%Y', 'now')";
    }
}
