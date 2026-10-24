// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/** {@link WritePlansTest}'s write plans on DuckDB (in process). */
class WritePlansDuckDbTest {

    @Test
    void plans() throws Exception {
        WritePlansTest.plans(TestBackends.of(DatabaseType.DuckDB));
    }
}
