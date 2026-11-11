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

    /** A CSV with an embedded JSON column (item 5d), on Postgres: its JSONB takes the cells. */
    @Test
    void aCsvsJsonColumn() throws Exception {
        CsvJsonColumnTest.onPostgres();
    }

    /**
     * A data: source's session table is the session's own on Postgres (the B2a audit's S4, 2026-11-11: the measurement
     * of Postgres's {@code SqlDialect.dataTable}): a user's table of its name in the session's own schema is neither
     * read nor dropped -- the table made TEMPORARY and every statement naming it qualified to {@code pg_temp} (the
     * second audit's S10, 2026-11-11), so a session whose search_path puts {@code pg_temp} after the user's schema still
     * reads its own table -- and the session holds no table of the name once the request lets it go.
     */
    @Test
    void aDataTableIsTheSessionsOwn() throws Exception {
        String content = DataDeliveryMatrixTest.PAYLOAD.substring(DataDeliveryMatrixTest.PAYLOAD.indexOf(',') + 1);
        String table = "legend_data_" + com.legend.cache.Hash.ofUtf8(content).hex().substring(0, 16);
        DataDeliveryMatrixTest.Data data = new DataDeliveryMatrixTest.Data(DataDeliveryMatrixTest.model(
                DataDeliveryMatrixTest.bareConnection(DatabaseType.Postgres), ""), "|sourceUrl('"
                + DataDeliveryMatrixTest.PAYLOAD + "')" + DataDeliveryMatrixTest.FROM_JSON, DataDeliveryMatrixTest.ROWS,
                "x::RT");
        try (java.sql.Connection c = DataDeliveryMatrixTest.session(DatabaseType.Postgres);
             java.sql.Statement s = c.createStatement()) {
            // the user's table of the name, holding a row of the user's
            s.execute("CREATE TABLE " + table + " (data TEXT)");
            s.execute("INSERT INTO " + table + " VALUES ('the user''s row')");
            // an unqualified name would now find the user's table first (S10's witness)
            s.execute("SET search_path TO public, pg_temp");
            com.legend.result.Collector collector = new com.legend.result.Collector();
            com.legend.Execution.run(com.legend.Compiler.query(com.legend.Compiler.compileModel(data.model()),
                            data.query()), data.runtime(),
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)),
                    com.legend.exec.PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                    com.legend.exec.ProductRun.Establish.DECLARED_SETUP, new com.legend.result.Output.Rows(collector));
            org.junit.jupiter.api.Assertions.assertEquals(DataDeliveryMatrixTest.ROWS,
                    DataDeliveryMatrixTest.strings(collector));
            // the user's table as it was
            try (java.sql.ResultSet r = s.executeQuery("SELECT data FROM " + table)) {
                org.junit.jupiter.api.Assertions.assertTrue(r.next());
                org.junit.jupiter.api.Assertions.assertEquals("the user's row", r.getString(1));
                org.junit.jupiter.api.Assertions.assertFalse(r.next());
            }
            // the session's own table of the name, gone with the request
            try (java.sql.ResultSet r = s.executeQuery("SELECT count(*) FROM pg_tables WHERE tablename = '" + table
                    + "' AND schemaname LIKE 'pg_temp%'")) {
                org.junit.jupiter.api.Assertions.assertTrue(r.next());
                org.junit.jupiter.api.Assertions.assertEquals(0, r.getInt(1));
            }
        }
    }
}
