// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/**
 * {@link RelationToOneGuardTest}'s guard on Postgres 16 (the embedded server). Its own target
 * ({@code //core:relation_to_one_postgres_test}): it needs the embedded Postgres in its runfiles.
 */
class RelationToOneGuardPostgresTest {

    @Test
    void guarded() throws Exception {
        RelationToOneGuardTest.guarded(TestBackends.of(DatabaseType.Postgres));
    }
}
