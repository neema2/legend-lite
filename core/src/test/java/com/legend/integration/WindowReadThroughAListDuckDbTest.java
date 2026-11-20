// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** {@link WindowReadThroughAList}'s shapes on DuckDB, whose list operations are functions: PCT's rows, each. */
class WindowReadThroughAListDuckDbTest {

    @Test
    void answered() throws Exception {
        WindowReadThroughAList.answered(TestBackends.of(DatabaseType.DuckDB), Map.of());
    }
}
