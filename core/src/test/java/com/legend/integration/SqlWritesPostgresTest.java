// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/**
 * {@link SqlWritesExecutionTest}'s writes on Postgres 16 (the embedded server), the island's own database. Run by
 * {@code //core:store_writes_postgres_test}: it needs the embedded Postgres in its runfiles.
 */
class SqlWritesPostgresTest {

    @Test
    void writes() throws Exception {
        SqlWritesExecutionTest.writes(TestBackends.of(DatabaseType.Postgres), true);
    }
}
