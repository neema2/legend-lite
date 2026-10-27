// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/** {@link SqlWritesExecutionTest}'s writes on H2. */
class SqlWritesH2Test {

    @Test
    void writes() throws Exception {
        // (H2 reaches no outer row from a derived table: a SET's correlated scalar subquery is its error there)
        SqlWritesExecutionTest.writes(TestBackends.of(DatabaseType.H2), false);
    }
}
