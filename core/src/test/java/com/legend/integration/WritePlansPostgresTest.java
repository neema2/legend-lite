// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/**
 * {@link WritePlansTest}'s write plans on Postgres 16 (the embedded server). Its own target's
 * ({@code //core:store_writes_postgres_test}): it needs the embedded Postgres in its runfiles.
 */
class WritePlansPostgresTest {

    @Test
    void plans() throws Exception {
        WritePlansTest.plans(TestBackends.of(DatabaseType.Postgres));
    }
}
