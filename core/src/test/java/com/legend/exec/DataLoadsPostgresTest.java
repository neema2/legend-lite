// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

/**
 * A CSV file's load on the embedded Postgres 16 (the user's ruling of 2026-11-10, "Okay I guess that can be used for
 * databases that have no csv read then"): the runner's cells by upstream's rule, streamed into Postgres's insert, at
 * every edge of {@code DataLoadsTest}'s pins, each answering as upstream answers.
 */
class DataLoadsPostgresTest {

    @Test
    void postgresLoadsUpstreamsCells(@TempDir Path dir) throws Exception {
        DataLoadsTest.pinsHold(List.of(TestBackends.of(DatabaseType.Postgres)), dir);
    }
}
