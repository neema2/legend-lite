// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The test-data generator's product doors (B2d.3e, 2026-11-12; {@link TestDataRoutes}): legend-engine's four routes on
 * a DuckDB file, each through {@link PureV1Api#route} as the server routes it, its run the server's door
 * ({@link ConnectionResolver#RUNNER}).
 */
class TestDataRoutesTest {

    private static String model;

    @BeforeAll
    static void setup() throws Exception {
        Path file = Files.createTempFile("tdg-routes-", ".duckdb");
        Files.delete(file);
        model = """
                ###Pure
                Class model::Firm { legalName: String[1]; }
                Class model::Person { name: String[1]; firm: model::Firm[0..1]; }

                ###Relational
                Database store::DB (
                    Table PERSON ( ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER )
                    Table FIRM ( ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100) )
                    Join PersonFirm(PERSON.FIRM_ID = FIRM.ID)
                )

                ###Mapping
                Mapping model::M (
                    model::Person: Relational {
                        ~mainTable [store::DB] PERSON
                        name: [store::DB] PERSON.NAME,
                        firm: [store::DB] @PersonFirm
                    }
                    model::Firm: Relational {
                        ~mainTable [store::DB] FIRM
                        legalName: [store::DB] FIRM.LEGAL_NAME
                    }
                )

                ###Connection
                RelationalDatabaseConnection store::Conn {
                    type: DuckDB;
                    specification: DuckDB { path: '%s'; };
                    auth: Test;
                }

                ###Runtime
                Runtime model::RT {
                    mappings: [ model::M ];
                    connections: [ store::DB: [ c: store::Conn ] ];
                }
                """.formatted(file.toString().replace("\\", "/"));
        Seed.sql(model, "CREATE TABLE PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER)",
                "model::RT");
        Seed.sql(model, "CREATE TABLE FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))", "model::RT");
        Seed.sql(model, "INSERT INTO PERSON VALUES (1, 'Ann', 10), (2, 'Bob', 20), (3, 'Cy', 10), (4, 'Di', NULL)",
                "model::RT");
        Seed.sql(model, "INSERT INTO FIRM VALUES (10, 'Acme'), (20, 'Bolt'), (30, 'Cog')", "model::RT");
    }

    private static final String QUERY = "|model::Person.all()->project([p|$p.name, p|$p.firm.legalName],"
            + " ['name', 'firm'])";

    /** An {@code ExecuteInput} of {@code query} over the model, its mapping and runtime named, and {@code more}. */
    private static String input(String query, String more) {
        return "{\"clientVersion\":\"vX_X_X\",\"function\":" + PureV1Api.grammarToJsonLambda(query, false).json()
                + ",\"model\":" + com.legend.json.Json.toCompact(Map.of("_type", "text", "code", model))
                + ",\"mapping\":\"model::M\",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"model::RT\"},"
                + "\"context\":{\"_type\":\"BaseExecutionContext\"}" + more + "}";
    }

    private static PureV1Api.Answer route(String path, String body) {
        return PureV1Api.route("/api/pure/v1/" + path, null, body, ConnectionResolver.RUNNER);
    }

    /** The test data from given identifiers: the routes' table order (by schema and table: FIRM before the root
     *  PERSON), as text. */
    @Test
    void withSeedAnswersTheTestDataInTheRoutesOrder() {
        PureV1Api.Answer a = route("execution/testDataGeneration/generateTestData_WithSeed", input(QUERY,
                ",\"tableRowIdentifiers\":[{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\","
                        + "\"database\":\"store::DB\"},\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"ID\","
                        + "\"value\":1}]},{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":2}]}]}]"));
        assertEquals(200, a.status(), a.json());
        assertEquals("text/plain", a.contentType());
        assertEquals("default\nFIRM\nID,LEGAL_NAME\n10,Acme\n20,Bolt\n-----\n"
                + "default\nPERSON\nFIRM_ID,ID,NAME\n10,1,Ann\n20,2,Bob\n-----\n", a.json());
    }

    /** The default seed: the query's own keys start the root (one statement); a seed the query reaches no row of
     *  starts it from every row, as upstream's empty filter does. */
    @Test
    void withDefaultSeedStartsFromTheQuerysKeys() {
        PureV1Api.Answer a = route("execution/testDataGeneration/generateTestData_WithDefaultSeed",
                input("|model::Person.all()->filter(p|$p.name != 'Bob')->project([p|$p.name, p|$p.firm.legalName],"
                        + " ['name', 'firm'])", ""));
        assertEquals(200, a.status(), a.json());
        assertEquals("default\nFIRM\nID,LEGAL_NAME\n10,Acme\n-----\n"
                + "default\nPERSON\nFIRM_ID,ID,NAME\n---null---,4,Di\n10,1,Ann\n10,3,Cy\n-----\n", a.json());
        PureV1Api.Answer none = route("execution/testDataGeneration/generateTestData_WithDefaultSeed",
                input("|model::Person.all()->filter(p|$p.name == 'Zed')->project([p|$p.name, p|$p.firm.legalName],"
                        + " ['name', 'firm'])", ""));
        assertEquals(200, none.status(), none.json());
        assertEquals("default\nFIRM\nID,LEGAL_NAME\n10,Acme\n20,Bolt\n-----\n"
                + "default\nPERSON\nFIRM_ID,ID,NAME\n---null---,4,Di\n10,1,Ann\n10,3,Cy\n20,2,Bob\n-----\n",
                none.json());
    }

    /** No identifiers for the root: upstream's error node's answer, its message and its example (no keys: S300). */
    @Test
    void withSeedAndNoIdentifiersIsUpstreamsErrorNode() {
        PureV1Api.Answer a = route("execution/testDataGeneration/generateTestData_WithSeed", input(QUERY, ""));
        assertEquals(500, a.status(), a.json());
        assertEquals("text/plain", a.contentType());
        assertEquals("Row Identifers should be provided for the root table: store::DB.PERSON [ID:INTEGER]\nExample:\n"
                + "{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\",\"database\":"
                + "\"store::DB\"},\"rowIdentifiers\":[]}", a.json());
    }

    /** The seed's keys as upstream's JSON, built by the database, by key (S198). */
    @Test
    void seedDataIsTheSeedsKeysAsJson() {
        PureV1Api.Answer a = route("execution/testDataGeneration/generateSeedData",
                input("|model::Person.all()->filter(p|$p.name != 'Bob')->project([p|$p.name], ['name'])", ""));
        assertEquals(200, a.status(), a.json());
        assertEquals("{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\",\"database\":"
                + "\"store::DB\"},\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":1}]},"
                + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":3}]},"
                + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":4}]}]}", a.json());
    }

    /** The census: upstream's embedded data, a model fact (no run). */
    @Test
    void theCensusIsUpstreamsEmbeddedData() {
        String body = "{\"query\":" + PureV1Api.grammarToJsonLambda(QUERY, false).json() + ",\"model\":"
                + com.legend.json.Json.toCompact(Map.of("_type", "text", "code", model))
                + ",\"mapping\":\"model::M\",\"runtime\":\"model::RT\"}";
        PureV1Api.Answer a = PureV1Api.route("/api/pure/v1/testData/generation/DONOTUSE_generateTestData", null, body,
                (query, runtime, output) -> {
                    throw new AssertionError("the census runs nothing");
                });
        assertEquals(200, a.status(), a.json());
        assertTrue(a.json().startsWith("{\"data\":[{\"_type\":\"relationalCSVData\",\"tables\":[{\"schema\":"
                + "\"default\",\"table\":\"PERSON\",\"values\":"), a.json());
        assertTrue(a.json().contains("{\"schema\":\"default\",\"table\":\"FIRM\",\"values\":\"ID,LEGAL_NAME\"}"),
                a.json());
    }

    /** A runtime given inline: refused by name. */
    @Test
    void anInlineRuntimeIsRefusedByName() {
        String body = input(QUERY, "").replace("{\"_type\":\"runtimePointer\",\"runtime\":\"model::RT\"}",
                "{\"_type\":\"engineRuntime\",\"mappings\":[]}");
        PureV1Api.Answer a = route("execution/testDataGeneration/generateTestData_WithDefaultSeed", body);
        assertEquals(500, a.status(), a.json());
        assertTrue(a.json().contains("a runtime given inline (engineRuntime)"), a.json());
    }
}
