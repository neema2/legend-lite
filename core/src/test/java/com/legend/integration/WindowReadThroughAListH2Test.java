// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** {@link WindowReadThroughAList}'s shapes on H2, which has no list encoding: each refused by name, never answered. */
class WindowReadThroughAListH2Test {

    @Test
    void refused() throws Exception {
        Map<String, String> refused = new java.util.HashMap<>();
        WindowReadThroughAList.SHAPES.forEach(s -> refused.put(s, WindowReadThroughAList.NO_LIST_ENCODING));
        WindowReadThroughAList.answered(TestBackends.of(DatabaseType.H2), refused);
    }
}
