// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/**
 * {@link StoreWritesExecutionTest}'s writes on Postgres 16 (the embedded server): the standard statements Postgres's
 * dialect writes, its stored columns' names folded, run by the database, in one transaction per program. Its own target
 * ({@code //core:store_writes_postgres_test}): it needs the embedded Postgres in its runfiles.
 */
class StoreWritesPostgresTest {

    @Test
    void writes() throws Exception {
        StoreWritesExecutionTest.writes(TestBackends.of(DatabaseType.Postgres));
    }
}
