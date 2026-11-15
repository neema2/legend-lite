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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code data:} SOURCE ON EVERY SERVER DATABASE THIS SESSION RUNS (the user, 2026-11-11: "we need to make json work
 * everywhere"; the JSON pass, the same day: "move everything off temp table"): the payload read by the statement
 * itself, as its dialect delivers it ({@link SqlDialect#deliver}) -- one bound parameter the database splits on MariaDB
 * ({@code JSON_TABLE}), MySQL ({@code JSON_TABLE}'s ordinals and {@code JSON_EXTRACT}), Trino ({@code UNNEST} of
 * {@code json_parse}) and ClickHouse ({@code JSONExtractArrayRaw}), the statement's own documents on MongoDB
 * ({@code $documents}) -- no load and no session table anywhere; and on each SQL server the payload's elements read
 * back exactly, a frame's records with their places, an empty array no row
 * ({@code integration.JsonPayloadFormsTest#assertReadsBack}).
 */
class DataSourceServersTest {

    private static final String PAYLOAD = "data:application/json,[{\"ID\":2,\"NAME\":\"b\"},{\"ID\":1,\"NAME\":\"a\"}]";
    private static final String QUERY = "|sourceUrl('" + PAYLOAD + "')->extend(~[ID: r|cast(get($r.data, 'ID'),"
            + " @meta::pure::metamodel::type::Integer), NAME: r|cast(get($r.data, 'NAME'),"
            + " @meta::pure::metamodel::type::String)])->select(~[ID, NAME])->sort(~ID->ascending())->from("
            + StorelessRuntime.RUNTIME + ")";
    private static final String ROWS = "[{\"ID\":1,\"NAME\":\"a\"},{\"ID\":2,\"NAME\":\"b\"}]";

    @Test
    void mariaDbBindsThePayload() throws Exception {
        assertReads(DatabaseType.MariaDB);
    }

    @Test
    void mySqlBindsThePayload() throws Exception {
        assertReads(DatabaseType.MySQL);
    }

    @Test
    void trinoBindsThePayload() throws Exception {
        assertReads(DatabaseType.Trino);
    }

    @Test
    void clickHouseBindsThePayload() throws Exception {
        assertReads(DatabaseType.ClickHouse);
    }

    @Test
    void mongoDbReadsItsOwnDocuments() throws Exception {
        assertReads(DatabaseType.MongoDB);
    }

    /** An EMPTY payload on each server (the orchestrator's ruling, 2026-11-11: JSON everywhere, the empty payload too):
     *  the relation's columns and no row, on the server. */
    @Test
    void anEmptyPayloadReadsNoRowOnEachServer() throws Exception {
        for (DatabaseType type : java.util.List.of(DatabaseType.MariaDB, DatabaseType.MySQL, DatabaseType.Trino,
                DatabaseType.ClickHouse, DatabaseType.MongoDB)) {
            // an element source's payload is one JSON array (an empty text is refused by name, as on every database)
            assertEquals("[]", rows(type, QUERY.replace(PAYLOAD, "data:application/json,[]")), type.name());
        }
    }

    /** The payload's elements read back exactly on each SQL server: a quote, a backslash, a line break, nested and
     *  typed values, a string and a null element; a frame's records with their places; an empty array no row. */
    @Test
    void aPayloadReadsBackOnEachSqlServer() throws Exception {
        for (DatabaseType type : java.util.List.of(DatabaseType.MariaDB, DatabaseType.MySQL, DatabaseType.Trino,
                DatabaseType.ClickHouse)) {
            try (java.sql.Connection c = TestBackends.of(type).open()) {
                com.legend.integration.JsonPayloadFormsTest.assertReadsBack(type, c);
            }
        }
    }

    /** A payload past Trino's statement limit ({@code query.max-length}, the server's setting, 1,000,000 characters
     *  by default; the B checkpoint's correctness audit, C5, 2026-11-11: no longer refused at planning) is refused by
     *  Trino itself, told under the payload's planned label (LOW 4): its digest's head and its size. */
    @Test
    void trinosOwnStatementLimitRefusesAPayloadNamingIt() throws Exception {
        String element = "{\"t\":\"" + "x".repeat(1000) + "\"}";
        String array = "[" + String.join(",", java.util.Collections.nCopies(1000, element)) + "]";
        String digest = com.legend.cache.Hash.ofUtf8(array).hex();
        var refused = org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.DataError.class, () -> rows(
                DatabaseType.Trino, "|sourceUrl('data:application/json," + array + "')->from("
                        + StorelessRuntime.RUNTIME + ")"));
        assertTrue(refused.getMessage().startsWith("the statement reading the data: payload " + digest.substring(0, 16)
                + " (" + array.length() + " characters) was refused: "), refused.getMessage().substring(0,
                        Math.min(400, refused.getMessage().length())));
    }

    /**
     * A COLUMN IS NAMED EXACTLY AS WRITTEN ON CLICKHOUSE (the user's ruling of 2026-11-13: every dialect's identifier
     * escaped as its own lexer reads a delimited identifier). ClickHouse reads a backslash escape inside a quoted
     * identifier, and until then only the quote was doubled: an extended column named {@code a\" , version() AS v --}
     * ran {@code version()} as a column of its own, a name ending in a backslash left its identifier open, and a declared
     * name holding one named another column. Each now runs on the pinned ClickHouse and answers one column named
     * exactly as written, and no other.
     */
    @Test
    void clickHouseNamesEachColumnExactlyAsWritten() throws Exception {
        var backend = TestBackends.of(DatabaseType.ClickHouse);
        try (java.sql.Connection c = backend.open(); java.sql.Statement s = c.createStatement()) {
            // the test's own table, its two columns declared with a backslash (ClickHouse's own spelling of them)
            s.execute("DROP TABLE IF EXISTS IDENTS");
            s.execute("CREATE TABLE IDENTS (ID Int64, \"a\\\\b\" Int64, \"e\\\\\" Int64) ENGINE = Memory");
            s.execute("INSERT INTO IDENTS VALUES (1, 2, 3)");
        }
        String model = """
                ###Relational
                Database s::DB ( Table IDENTS ( ID INTEGER PRIMARY KEY, "a\\\\b" INTEGER, "e\\\\" INTEGER ) )
                ###Connection
                RelationalDatabaseConnection s::Conn { store: s::DB; %s }
                ###Runtime
                Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(backend.connectionBody());
        String breakout = "a\\\" , version() AS v --";
        Map<String, List<String>> asked = new java.util.LinkedHashMap<>();
        // a declared name holding a backslash, and one ending in one
        asked.put("|#>{s::DB.IDENTS}#", List.of("ID", "a\\b", "e\\"));
        // an extended column's name, the breakout's payload
        asked.put("|#>{s::DB.IDENTS}#->select(~[ID])->extend(~['a\\\\\" , version() AS v --':r|1])",
                List.of("ID", breakout));
        // a renamed column's, ending in a backslash
        asked.put("|#>{s::DB.IDENTS}#->select(~[ID])->rename(~ID, ~'z\\\\')", List.of("z\\"));
        for (Map.Entry<String, List<String>> a : asked.entrySet()) {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), a.getKey()).executionPlan("s::RT");
            StringWriter out = new StringWriter();
            try (java.sql.Connection c = backend.open()) {
                PlanRunner.stream(plan, Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                        PlanCases.Written.JSON.writer(out, plan.answerShape()), java.util.UUID.randomUUID());
            }
            var rows = ((com.legend.json.Json.Arr) com.legend.json.Json.parse(out.toString())).items();
            assertEquals(1, rows.size(), a.getKey() + ": " + out);
            assertEquals(a.getValue(), List.copyOf(((com.legend.json.Json.Obj) rows.get(0)).fields().keySet()),
                    a.getKey() + ": its columns, each named exactly as written, and no other: " + out);
        }
    }

    /** {@code type}'s plan of {@code query}, run: no load, its JSON rows. */
    private static String rows(DatabaseType type, String query) throws Exception {
        var backend = TestBackends.of(type);
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(backend.withStorelessRuntime("")), query)
                .executionPlan(StorelessRuntime.RUNTIME);
        StringWriter out = new StringWriter();
        if (type == DatabaseType.MongoDB) {
            PlanRunner.stream(plan, Map.of(), PlanSessions.shared(), PlanCases.Written.JSON.writer(out,
                    plan.answerShape()), java.util.UUID.randomUUID());
        } else {
            try (java.sql.Connection c = backend.open()) {
                PlanRunner.stream(plan, Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                        PlanCases.Written.JSON.writer(out, plan.answerShape()), java.util.UUID.randomUUID());
            }
        }
        return out.toString();
    }

    /** {@code type}'s plan of the query: no load and no session table, the payload its statement's own -- one bound
     *  parameter, or MongoDB's documents -- answering the payload's rows, in order. */
    private static void assertReads(DatabaseType type) throws Exception {
        var backend = TestBackends.of(type);
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(backend.withStorelessRuntime("")), QUERY)
                .executionPlan(StorelessRuntime.RUNTIME);
        String json = com.legend.executionplan.PlanJson.write(plan);
        assertTrue(!json.contains("legend_data_") && plan.answerSql().target().loads().isEmpty(),
                type + ": no session table, no load: " + json);
        boolean bound = backend.dialect().deliver(SqlDialect.DataPayload.alone(new com.legend.sql.SqlSource.SourceUrl(
                "data:application/json,[1]", "t0", List.of(new com.legend.sql.OutputCol("data",
                        com.legend.sql.SqlType.Scalar.JSON, true, com.legend.sql.OutputCol.Origin.DERIVED)),
                com.legend.sql.SqlSource.SourceUrl.Form.ELEMENTS), List.of("1")))
                instanceof com.legend.sql.SqlSource.SourceUrl;
        assertEquals(type != DatabaseType.MongoDB, bound, type + "'s delivery, as its dialect answers");
        assertEquals(bound, plan.answerSql().slots().stream().anyMatch(sl -> sl.binding()
                instanceof ExecutionPlan.Binding.Payload), type + ": the payload is a bound slot: " + json);
        assertEquals(ROWS, rows(type, QUERY), type + ": the payload's rows, sorted");
    }
}
