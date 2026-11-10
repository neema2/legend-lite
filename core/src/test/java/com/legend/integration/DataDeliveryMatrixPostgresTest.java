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
            // A2 and A5: a from() naming its runtime, refused on the product's doors and with no program runtime
            "matrix:READ/FROM_SEED/Postgres", "matrix:WRITE/FROM_SEED/Postgres", "matrix:PLAN/FROM_SEED/Postgres",
            "matrix:TEST_BODY/FROM_SEED/Postgres",
            // A3: a database with no file reader refuses a file by name (option 1); Postgres streams it today
            "matrix:TEST_BODY/CSV_FILE/Postgres",
            // A4: a bad value's refusal names no table
            "failure:BAD_VALUE/Postgres",
            // A5: a write on the caller's session crashes on the target's declared test data, and reads the catalog on
            // the user's session
            "matrix:WRITE/CSV_TEST_DATA/Postgres", "matrix:WRITE/SQL_TEST_DATA/Postgres", "matrix:WRITE/CATALOG/Postgres",
            // B4 (every catalog statement on the process's in-process catalog): a test body's and a service's catalog
            // read on a server database asks for a system connection none configured
            "matrix:TEST_BODY/CATALOG/Postgres", "matrix:SERVICE/CATALOG/Postgres");

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
