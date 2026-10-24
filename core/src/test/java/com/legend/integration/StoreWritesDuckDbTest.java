// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/** {@link StoreWritesExecutionTest}'s writes on DuckDB (in process). */
class StoreWritesDuckDbTest {

    @Test
    void writes() throws Exception {
        StoreWritesExecutionTest.writes(TestBackends.of(DatabaseType.DuckDB));
    }
}
