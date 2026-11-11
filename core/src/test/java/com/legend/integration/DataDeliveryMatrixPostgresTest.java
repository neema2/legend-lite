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
            // (B4, 2026-11-11: every catalog statement on the process's in-process catalog -- a test body's, a service's
            // and a program's catalog read on Postgres green, its three cells taken off: WRITE, TEST_BODY and SERVICE)
            );

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

    /** A catalog read an assert expects to fail, then another, in a body on Postgres -- whose catalog is the platform's
     *  own engine's (B4) -- alone and beside the body's own table (the B4 audit's finding 2). */
    @Test
    void aFailedGuardedCatalogReadLeavesTheNextOneAnswered(@TempDir Path dir) throws Exception {
        CatalogTurnFailureTest.assertPasses(CatalogTurnFailureTest.alone(DatabaseType.Postgres), DatabaseType.Postgres,
                dir);
        CatalogTurnFailureTest.assertPasses(CatalogTurnFailureTest.mixed(DatabaseType.Postgres), DatabaseType.Postgres,
                dir);
    }

    /** A CSV with an embedded JSON column (item 5d), on Postgres: its JSONB takes the cells. */
    @Test
    void aCsvsJsonColumn() throws Exception {
        CsvJsonColumnTest.onPostgres();
    }

    /**
     * A data: payload read back exactly on Postgres (the JSON pass, 2026-11-11: one bound parameter, split by
     * {@code jsonb_array_elements}; until then a session table in {@code pg_temp}, which the B2a audit's S4 and the
     * second audit's S10 measured here): a quote, a backslash, a line break, nested and typed values, a string and a null
     * element; a frame's records with their places; an empty array no row ({@link JsonPayloadFormsTest}).
     */
    @Test
    void aPayloadReadsBack() throws Exception {
        try (java.sql.Connection c = DataDeliveryMatrixTest.session(DatabaseType.Postgres)) {
            JsonPayloadFormsTest.assertReadsBack(DatabaseType.Postgres, c);
        }
    }
}
