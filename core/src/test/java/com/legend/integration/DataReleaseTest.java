// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.exec.PlanSessions;
import com.legend.exec.ProductRun;
import com.legend.exec.SessionSource;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.result.Collector;
import com.legend.result.Output;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE QUERY DOOR'S RELEASE (the second audit's S12 and S11, 2026-11-11): a writing program's {@code data:} tables are
 * dropped by its plan's release, sent once the run is over WHATEVER ITS OUTCOME -- until then the drops were the
 * program's last steps, never sent when a step before them failed, and a table a rollback does not take back (H2's,
 * MariaDB's) stayed in the session -- and AFTER the run's rollback, so a drop that commits an open transaction (H2's)
 * commits none of the run's writes. A program that writes a row, then fails on a duplicate key: neither the row nor
 * a data: table is left. (On DuckDB and H2, where the store writes run; SQLite spells no write of a query's rows.)
 */
class DataReleaseTest {

    /** A third row's payload. */
    private static final String THIRD = "data:application/json,[{\"ID\":3,\"NAME\":\"c\"}]";

    /** A new row written (key 3), then the payload written over the declared rows' keys (1 and 2): the second write
     *  fails at run, and the run's transaction takes the first back. */
    private static final String PROGRAM = "{|sourceUrl('" + THIRD + "')" + DataDeliveryMatrixTest.FROM_JSON
            + "->write(#>{x::DB.T}#); sourceUrl('" + DataDeliveryMatrixTest.PAYLOAD + "')"
            + DataDeliveryMatrixTest.FROM_JSON + "->write(#>{x::DB.T}#);}";

    @Test
    void aFailedProgramsDataTablesAreReleased() throws Exception {
        String content = DataDeliveryMatrixTest.PAYLOAD.substring(DataDeliveryMatrixTest.PAYLOAD.indexOf(',') + 1);
        String digest = com.legend.cache.Hash.ofUtf8(content).hex().substring(0, 16);
        for (DatabaseType db : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            String model = DataDeliveryMatrixTest.model(DataDeliveryMatrixTest.connection(db,
                    DataDeliveryMatrixTest.localH2("testDataSetupCSV: 'default\\nT\\n" + DataDeliveryMatrixTest.ROWS_CSV
                            + "'")), "");
            var query = Compiler.query(Compiler.compileModel(model), PROGRAM);
            // the plan: its drops are its release, no step of its own
            ExecutionPlan plan = query.executionPlan("x::RT");
            assertNotNull(plan.release(), db + ": the program's data: tables are released by its plan");
            assertTrue(plan.release().statements().stream().anyMatch(s -> s.toLowerCase(java.util.Locale.ROOT)
                    .matches("drop .*legend_data_" + digest + ".*")), db + ": its table's drop: " + plan.release());
            assertFalse(com.legend.executionplan.PlanJson.write(plan).isEmpty());
            assertEquals(plan.release(), com.legend.executionplan.PlanJson.read(
                    com.legend.executionplan.PlanJson.write(plan)).release(), db + ": the release round-trips");
            // the run: the write fails, the release is sent after
            try (Connection c = DataDeliveryMatrixTest.session(db); Statement s = c.createStatement()) {
                RuntimeException failed = assertThrows(RuntimeException.class, () -> Execution.run(query, "x::RT",
                        SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                        ProductRun.Establish.DECLARED_SETUP, new Output.Rows(new Collector())), db.name());
                assertTrue(String.valueOf(failed.getMessage()).toLowerCase(java.util.Locale.ROOT).matches("(?s).*(primary|unique|constraint|duplicate).*"),
                        db + ": the write's key violation: " + failed.getMessage());
                assertEquals(0, tablesNamed(db, s, "legend_data_" + digest),
                        db + ": the session holds no data: table after the failed run");
                String third = com.legend.cache.Hash.ofUtf8(THIRD.substring(THIRD.indexOf(',') + 1)).hex()
                        .substring(0, 16);
                assertEquals(0, tablesNamed(db, s, "legend_data_" + third), db + ": nor the first write's");
                try (ResultSet r = s.executeQuery("SELECT count(*) FROM T WHERE ID = 3")) {
                    r.next();
                    assertEquals(0, r.getInt(1), db + ": the first write rolled back, the release after the rollback");
                }
            }
        }
    }

    /** How many tables of {@code name} the session holds, temporary ones included. */
    private static int tablesNamed(DatabaseType db, Statement s, String name) throws java.sql.SQLException {
        String sql = switch (db) {
            case DuckDB -> "SELECT count(*) FROM duckdb_tables() WHERE lower(table_name) = '" + name + "'";
            case H2 -> "SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE lower(TABLE_NAME) = '" + name + "'";
            case SQLite -> "SELECT count(*) FROM (SELECT name FROM sqlite_temp_master UNION ALL SELECT name FROM"
                    + " sqlite_master) WHERE lower(name) = '" + name + "'";
            default -> throw new IllegalArgumentException(db.name());
        };
        try (ResultSet r = s.executeQuery(sql)) {
            r.next();
            return r.getInt(1);
        }
    }
}
