// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code data:} SOURCE ON EVERY SERVER DATABASE THIS SESSION RUNS (the user, 2026-11-11: "we need to make json work
 * everywhere"; docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6): the payload's elements read as a relation on each embedded
 * server, by the delivery its dialect declares ({@link SqlDialect#dataDelivery}) -- a session table the plan's load
 * makes on MariaDB and MySQL (their {@code TEMPORARY} table, measured), the statement's own rows on Trino (VALUES),
 * ClickHouse ({@code values}) and MongoDB ({@code $documents}), which hold no session table a plan can name.
 */
class DataSourceServersTest {

    private static final String PAYLOAD = "data:application/json,[{\"ID\":2,\"NAME\":\"b\"},{\"ID\":1,\"NAME\":\"a\"}]";
    private static final String QUERY = "|sourceUrl('" + PAYLOAD + "')->extend(~[ID: r|cast(get($r.data, 'ID'),"
            + " @meta::pure::metamodel::type::Integer), NAME: r|cast(get($r.data, 'NAME'),"
            + " @meta::pure::metamodel::type::String)])->select(~[ID, NAME])->sort(~ID->ascending())->from("
            + StorelessRuntime.RUNTIME + ")";
    private static final String ROWS = "[{\"ID\":1,\"NAME\":\"a\"},{\"ID\":2,\"NAME\":\"b\"}]";

    @Test
    void mariaDbReadsItsSessionTable() throws Exception {
        assertReads(DatabaseType.MariaDB, SqlDialect.DataDelivery.SESSION_TABLE);
    }

    @Test
    void mySqlReadsItsSessionTable() throws Exception {
        assertReads(DatabaseType.MySQL, SqlDialect.DataDelivery.SESSION_TABLE);
    }

    @Test
    void trinoReadsItsOwnRows() throws Exception {
        assertReads(DatabaseType.Trino, SqlDialect.DataDelivery.INLINE);
    }

    @Test
    void clickHouseReadsItsOwnRows() throws Exception {
        assertReads(DatabaseType.ClickHouse, SqlDialect.DataDelivery.INLINE);
    }

    @Test
    void mongoDbReadsItsOwnDocuments() throws Exception {
        assertReads(DatabaseType.MongoDB, SqlDialect.DataDelivery.INLINE);
    }

    /** {@code type}'s plan of the query: delivered as {@code delivery} says -- a load making the session table, or no
     *  load and the payload in the statement -- and answering the payload's rows, in order. */
    private static void assertReads(DatabaseType type, SqlDialect.DataDelivery delivery) throws Exception {
        var backend = TestBackends.of(type);
        assertEquals(delivery, backend.dialect().dataDelivery(), type + "'s declared delivery");
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(backend.withStorelessRuntime("")), QUERY)
                .executionPlan(StorelessRuntime.RUNTIME);
        String json = com.legend.executionplan.PlanJson.write(plan);
        if (delivery == SqlDialect.DataDelivery.SESSION_TABLE) {
            assertTrue(json.contains("legend_data_"), type + ": the payload's session table is the plan's load: "
                    + json);
        } else {
            assertTrue(!json.contains("legend_data_"), type + ": no session table: " + json);
        }
        StringWriter out = new StringWriter();
        if (delivery == SqlDialect.DataDelivery.INLINE) {
            // no load: the plan streams on the session its target names (MongoDB's connector), or on the test's own
            if (type == DatabaseType.MongoDB) {
                PlanRunner.stream(plan, Map.of(), PlanSessions.shared(), PlanCases.Written.JSON.writer(out,
                        plan.answerShape()));
            } else {
                try (java.sql.Connection c = backend.open()) {
                    PlanRunner.stream(plan, Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                            PlanCases.Written.JSON.writer(out, plan.answerShape()));
                }
            }
        } else {
            // a fresh session on the server's test database, the test's own (a declared URL takes no load of a plan's):
            // the target's loads -- the session table made, and dropped when the request lets the session go -- run on
            // it, as the product's door runs them on a session it is given
            try (java.sql.Connection c = backend.open()) {
                com.legend.result.Collector rows = new com.legend.result.Collector();
                com.legend.Execution.run(Compiler.query(Compiler.compileModel(backend.withStorelessRuntime("")),
                                QUERY), StorelessRuntime.RUNTIME,
                        SessionSource.given(com.legend.testbackend.TestSessions.over(c)),
                        PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                        ProductRun.Establish.DECLARED_SETUP, new com.legend.result.Output.Rows(rows));
                assertEquals("[[1, a], [2, b]]", String.valueOf(rows.collected().rows().stream().map(r -> r.stream()
                        .map(String::valueOf).toList()).toList()), type + ": the payload's rows, sorted");
                // the session table released with the request: the session holds none of the name
                try (java.sql.Statement st = c.createStatement()) {
                    String table = json.substring(json.indexOf("legend_data_"), json.indexOf("legend_data_") + 28);
                    java.sql.SQLException gone = org.junit.jupiter.api.Assertions.assertThrows(
                            java.sql.SQLException.class, () -> st.executeQuery("SELECT 1 FROM " + table));
                    assertTrue(gone.getMessage().contains("doesn't exist"), type + ": " + gone.getMessage());
                }
            }
            return;
        }
        assertEquals(ROWS, out.toString(), type + ": the payload's rows, sorted");
    }
}
