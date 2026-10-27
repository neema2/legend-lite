// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/** {@link CypherDomainWritesTest}'s writes on H2 (in process), whose writes over a domain model had raised "Function
 *  REVERSE not found" (PR #67's review, 2026-10-22: the id read from an identity's text reversed it). */
class CypherDomainWritesH2Test {

    @Test
    void writes() throws Exception {
        CypherDomainWritesTest.writes(TestBackends.of(DatabaseType.H2));
    }
}
