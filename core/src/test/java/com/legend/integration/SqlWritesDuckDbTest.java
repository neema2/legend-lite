// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/** {@link SqlWritesExecutionTest}'s writes on DuckDB. */
class SqlWritesDuckDbTest {

    @Test
    void writes() throws Exception {
        SqlWritesExecutionTest.writes(TestBackends.of(DatabaseType.DuckDB), true);
    }
}
