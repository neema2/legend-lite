// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/**
 * {@link WindowReadThroughAList}'s shapes on Postgres 16 (the embedded server), where a list operation is a query over
 * {@code unnest}: PCT's rows, each (the chunked form was refused before 2026-11-19, "window functions are not allowed
 * in functions in FROM"). Its own target ({@code //core:window_read_through_a_list_postgres_test}): it needs the
 * embedded Postgres in its runfiles.
 */
class WindowReadThroughAListPostgresTest {

    @Test
    void answered() throws Exception {
        WindowReadThroughAList.answered(TestBackends.of(DatabaseType.Postgres), java.util.Map.of());
    }
}
