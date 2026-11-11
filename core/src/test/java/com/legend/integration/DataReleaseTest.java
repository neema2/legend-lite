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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NO RELEASE EXISTS (the JSON pass, 2026-11-11; until then the second audit's S12 and S11: a writing program's
 * {@code data:} tables were dropped by its plan's release, sent once the run was over whatever its outcome): a payload is
 * read by the statement itself -- one bound parameter, or the statement's own rows -- so a program that reads payloads
 * plans no load, no session table and nothing to send after its run. A program that writes a row, then fails on a
 * duplicate key: its plan holds its steps alone, the row is rolled back with the run, and the session holds no table of
 * the payloads'. (On DuckDB and H2, where the store writes run; SQLite spells no write of a query's rows.)
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
    void aProgramReadingPayloadsPlansNoLoadAndNoRelease() throws Exception {
        for (DatabaseType db : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            String model = DataDeliveryMatrixTest.model(DataDeliveryMatrixTest.connection(db,
                    DataDeliveryMatrixTest.localH2("testDataSetupCSV: 'default\\nT\\n" + DataDeliveryMatrixTest.ROWS_CSV
                            + "'")), "");
            var query = Compiler.query(Compiler.compileModel(model), PROGRAM);
            ExecutionPlan plan = query.executionPlan("x::RT");
            String json = com.legend.executionplan.PlanJson.write(plan);
            assertFalse(json.contains("\"release\"") || json.toLowerCase(java.util.Locale.ROOT).contains("legend_data_"),
                    db + ": no release and no session table: " + json);
            for (ExecutionPlan.Node step : ((ExecutionPlan.Sequence) plan.root()).steps()) {
                assertFalse(step instanceof ExecutionPlan.Load, db + ": no load of a payload's: " + step);
            }
            // the run: the write fails, the first write rolled back with the run; no table of a payload's
            try (Connection c = DataDeliveryMatrixTest.session(db); Statement s = c.createStatement()) {
                RuntimeException failed = assertThrows(RuntimeException.class, () -> Execution.run(query, "x::RT",
                        SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                        ProductRun.Establish.DECLARED_SETUP, new Output.Rows(new Collector())), db.name());
                assertTrue(String.valueOf(failed.getMessage()).toLowerCase(java.util.Locale.ROOT).matches("(?s).*(primary|unique|constraint|duplicate).*"),
                        db + ": the write's key violation: " + failed.getMessage());
                assertEquals(0, tablesNamed(db, s, "legend_data_%"), db + ": the session holds no payload's table");
                try (ResultSet r = s.executeQuery("SELECT count(*) FROM T WHERE ID = 3")) {
                    r.next();
                    assertEquals(0, r.getInt(1), db + ": the first write rolled back with the run");
                }
            }
        }
    }

    /** How many tables whose name is like {@code pattern} the session holds, temporary ones included. */
    private static int tablesNamed(DatabaseType db, Statement s, String pattern) throws java.sql.SQLException {
        String sql = switch (db) {
            case DuckDB -> "SELECT count(*) FROM duckdb_tables() WHERE lower(table_name) LIKE '" + pattern + "'";
            case H2 -> "SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE lower(TABLE_NAME) LIKE '" + pattern + "'";
            default -> throw new IllegalArgumentException(db.name());
        };
        try (ResultSet r = s.executeQuery(sql)) {
            r.next();
            return r.getInt(1);
        }
    }
}
