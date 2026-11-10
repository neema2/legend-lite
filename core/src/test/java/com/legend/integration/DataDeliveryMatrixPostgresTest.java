// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * THE DATA DELIVERY MATRIX ON POSTGRES (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §5, A1): the in-process matrix's cells,
 * failure cases and type round trip on the embedded Postgres 16, run as an unprivileged user (Postgres refuses root:
 * {@code --run_under} the scratch wrapper, docs/CLOUD_SESSIONS.md), with Postgres's own red list.
 */
class DataDeliveryMatrixPostgresTest {

    private static final List<DatabaseType> POSTGRES = List.of(DatabaseType.Postgres);

    /**
     * Postgres's red cells, measured 2026-11-10 on 886eadde + the design merge (A1), each taken off by the step that
     * fixes it; the list only shrinks. The server's project road is not run on Postgres ({@code DataDeliveryMatrixTest
     * .runs}: the harness's declared test connection has no role on the embedded server).
     */
    static final Set<String> RED = Set.of(
            // (A3, 2026-11-10: Postgres refuses a file by name, option 1, where it streamed it)
            // (a bad value's refusal names its table and column since A2, 2026-11-10: refused at planning)
            // (A5, 2026-11-10: a query given no runtime runs on the one its from()s name; a write on the caller's session
            // establishes its declared setup as a read does)
            // B4 (every catalog statement on the process's in-process catalog): a test body's and a service's catalog
            // read on a server database asks for a system connection none configured; a program reading the store on a
            // server database is refused by name (A5: the catalog is made in process, and the server's form is a
            // query's alone)
            "matrix:WRITE/CATALOG/Postgres", "matrix:TEST_BODY/CATALOG/Postgres", "matrix:SERVICE/CATALOG/Postgres");

    @Test
    void theMatrix(@TempDir Path dir) throws Exception {
        DataDeliveryMatrixTest.matrix(POSTGRES, dir, RED);
    }

    @Test
    void theFailureCases(@TempDir Path dir) throws Exception {
        DataDeliveryMatrixTest.failures(POSTGRES, dir, RED);
    }

    @Test
    void theTypeRoundTrip() throws Exception {
        DataDeliveryMatrixTest.roundTrip(POSTGRES, RED, false);
    }
}
