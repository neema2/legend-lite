// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;

/** {@link HardeningSuite} on the platform's database (DuckDB, in process). */
class GraphQLHardeningTest extends HardeningSuite {

    @Override
    TestBackend backend() {
        return TestBackends.platform();
    }
}
