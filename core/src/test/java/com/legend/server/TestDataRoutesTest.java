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
                Class model::PersonView { name: String[1]; }

                ###Relational
                Database store::DB (
                    Table PERSON ( ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER )
                    Table FIRM ( ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100) )
                    Join PersonFirm(PERSON.FIRM_ID = FIRM.ID)
                    Filter Early(PERSON.ID < 4)
                    View PERSON_VIEW ( ~filter Early ID: PERSON.ID PRIMARY KEY, NAME: PERSON.NAME )
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
                    model::PersonView: Relational {
                        ~mainTable [store::DB] PERSON_VIEW
                        name: [store::DB] PERSON_VIEW.NAME
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

                Runtime model::Unmapped {
                    mappings: [ ];
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
                + "default\nPERSON\nFIRM_ID,ID,NAME\n10,1,Ann\n10,3,Cy\n---null---,4,Di\n-----\n", a.json());
        PureV1Api.Answer none = route("execution/testDataGeneration/generateTestData_WithDefaultSeed",
                input("|model::Person.all()->filter(p|$p.name == 'Zed')->project([p|$p.name, p|$p.firm.legalName],"
                        + " ['name', 'firm'])", ""));
        assertEquals(200, none.status(), none.json());
        assertEquals("default\nFIRM\nID,LEGAL_NAME\n10,Acme\n20,Bolt\n-----\n"
                + "default\nPERSON\nFIRM_ID,ID,NAME\n10,1,Ann\n10,3,Cy\n20,2,Bob\n---null---,4,Di\n-----\n",
                none.json());
    }

    /** No identifiers for the root: upstream's error node's answer, its message and its example (no keys: S300). */
    @Test
    void withSeedAndNoIdentifiersIsUpstreamsErrorNode() {
        PureV1Api.Answer a = route("execution/testDataGeneration/generateTestData_WithSeed", input(QUERY, ""));
        assertEquals(500, a.status(), a.json());
        assertEquals("text/plain", a.contentType());
        assertEquals("Row Identifers should be provided for the root table: store::DB.PERSON [ID:INT]\nExample:\n"
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

    /** The routes print a timestamp as legend-engine does (L9, 2026-11-12: the routes are its API, so backwards
     *  compatibility's printing): the test data's cut to {@code yyyy-MM-dd HH:mm:ss}, upstream's
     *  {@code SimpleDateTimeFormat}; the seed's key in full, upstream's {@code ValueTransformer} text. */
    @Test
    void theRoutesPrintATimestampAsUpstreamDoes() throws Exception {
        Path file = Files.createTempFile("tdg-stamps-", ".duckdb");
        Files.delete(file);
        String stamps = """
                ###Pure
                Class model::E { n: Integer[0..1]; }

                ###Relational
                Database store::TS ( Table EVENT ( STAMP TIMESTAMP PRIMARY KEY, N INTEGER ) )

                ###Mapping
                Mapping model::EM ( model::E: Relational { ~mainTable [store::TS] EVENT n: [store::TS] EVENT.N } )

                ###Connection
                RelationalDatabaseConnection store::TsConn {
                    type: DuckDB;
                    specification: DuckDB { path: '%s'; };
                    auth: Test;
                }

                ###Runtime
                Runtime model::ERT {
                    mappings: [ model::EM ];
                    connections: [ store::TS: [ c: store::TsConn ] ];
                }
                """.formatted(file.toString().replace("\\", "/"));
        Seed.sql(stamps, "CREATE TABLE EVENT (STAMP TIMESTAMP PRIMARY KEY, N INTEGER)", "model::ERT");
        Seed.sql(stamps, "INSERT INTO EVENT VALUES (TIMESTAMP '2020-01-02 03:04:05.123', 1),"
                + " (TIMESTAMP '2020-01-03 00:00:00', 2)", "model::ERT");
        String in = "{\"clientVersion\":\"vX_X_X\",\"function\":"
                + PureV1Api.grammarToJsonLambda("|model::E.all()->project([e|$e.n], ['n'])", false).json()
                + ",\"model\":" + com.legend.json.Json.toCompact(Map.of("_type", "text", "code", stamps))
                + ",\"mapping\":\"model::EM\",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"model::ERT\"},"
                + "\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        PureV1Api.Answer data = route("execution/testDataGeneration/generateTestData_WithDefaultSeed", in);
        assertEquals(200, data.status(), data.json());
        assertEquals("default\nEVENT\nN,STAMP\n1,2020-01-02 03:04:05\n2,2020-01-03 00:00:00\n-----\n", data.json());
        PureV1Api.Answer seed = route("execution/testDataGeneration/generateSeedData", in);
        assertEquals(200, seed.status(), seed.json());
        assertEquals("{\"table\":{\"_type\":\"table\",\"table\":\"EVENT\",\"schema\":\"default\",\"database\":"
                + "\"store::TS\"},\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"STAMP\",\"value\":"
                + "\"2020-01-02T03:04:05.123000000+0000\"}]},{\"columnValuePairs\":[{\"name\":\"STAMP\",\"value\":"
                + "\"2020-01-03T00:00:00.000000000+0000\"}]}]}", seed.json());
    }

    /** A temporal key's value is the request's text: in its ISO shape (no quote) it is the column type's typed literal;
     *  any other text -- a quote's injection, a date in another shape -- is refused by name, never written into a
     *  statement (the final audit, 2026-11-12: until then the text went UNCHECKED into {@code DATE '<text>'}, so
     *  {@code 2024-01-01' OR '1'='1} answered every row). A timestamp key the seed route printed reads back its row. */
    @Test
    void aTemporalKeysValueIsTypedWhereItIsIsoAndRefusedByNameOtherwise() throws Exception {
        Path file = Files.createTempFile("tdg-days-", ".duckdb");
        Files.delete(file);
        String days = """
                ###Pure
                Class model::D { n: Integer[0..1]; }
                Class model::S { n: Integer[0..1]; }

                ###Relational
                Database store::DAYS ( Table DAY ( D DATE PRIMARY KEY, N INTEGER ) Table STAMP ( S TIMESTAMP PRIMARY KEY, N INTEGER ) )

                ###Mapping
                Mapping model::DM (
                    model::D: Relational { ~mainTable [store::DAYS] DAY n: [store::DAYS] DAY.N }
                    model::S: Relational { ~mainTable [store::DAYS] STAMP n: [store::DAYS] STAMP.N }
                )

                ###Connection
                RelationalDatabaseConnection store::DaysConn {
                    type: DuckDB;
                    specification: DuckDB { path: '%s'; };
                    auth: Test;
                }

                ###Runtime
                Runtime model::DRT {
                    mappings: [ model::DM ];
                    connections: [ store::DAYS: [ c: store::DaysConn ] ];
                }
                """.formatted(file.toString().replace("\\", "/"));
        Seed.sql(days, "CREATE TABLE DAY (D DATE PRIMARY KEY, N INTEGER)", "model::DRT");
        Seed.sql(days, "INSERT INTO DAY VALUES (DATE '2024-01-01', 1), (DATE '2024-01-02', 2)", "model::DRT");
        Seed.sql(days, "CREATE TABLE STAMP (S TIMESTAMP PRIMARY KEY, N INTEGER)", "model::DRT");
        Seed.sql(days, "INSERT INTO STAMP VALUES (TIMESTAMP '2020-01-02 03:04:05.123', 1),"
                + " (TIMESTAMP '2020-01-03 00:00:00', 2)", "model::DRT");
        java.util.function.BiFunction<String, String, PureV1Api.Answer> withKey = (table, value) -> route(
                "execution/testDataGeneration/generateTestData_WithSeed", "{\"clientVersion\":\"vX_X_X\",\"function\":"
                + PureV1Api.grammarToJsonLambda("|model::" + table.charAt(0) + ".all()->project([d|$d.n], ['n'])",
                        false).json()
                + ",\"model\":" + com.legend.json.Json.toCompact(Map.of("_type", "text", "code", days))
                + ",\"mapping\":\"model::DM\",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"model::DRT\"},"
                + "\"context\":{\"_type\":\"BaseExecutionContext\"},\"tableRowIdentifiers\":[{\"table\":{\"_type\":"
                + "\"table\",\"table\":\"" + table + "\",\"schema\":\"default\",\"database\":\"store::DAYS\"},"
                + "\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"" + (table.equals("DAY") ? "D" : "S")
                + "\",\"value\":" + com.legend.json.Json.toCompact(value) + "}]}]}]}");
        PureV1Api.Answer one = withKey.apply("DAY", "2024-01-02");
        assertEquals(200, one.status(), one.json());
        assertEquals("default\nDAY\nD,N\n2024-01-02,2\n-----\n", one.json(), "the valid date's row alone");
        for (String value : java.util.List.of("2024-01-01' OR '1'='1", "01/02/2024")) {
            PureV1Api.Answer refused = withKey.apply("DAY", value);
            assertTrue(refused.status() != 200, value + ": " + refused.json());
            assertTrue(refused.json().contains("testDataGen: the value '" + value.replace("'", "\\u0027")
                    + "' for the column D is no ISO date") || refused.json().contains("testDataGen: the value '"
                    + value + "' for the column D is no ISO date"), value + ": " + refused.json());
        }
        PureV1Api.Answer stamp = withKey.apply("STAMP", "2020-01-02T03:04:05.123000000+0000");
        assertEquals(200, stamp.status(), stamp.json());
        assertEquals("default\nSTAMP\nN,S\n1,2020-01-02 03:04:05\n-----\n", stamp.json(), "the seed's printed key");
    }

    /** A TIMESTAMP_NS key with nine fractional digits finds ITS row on DuckDB, not the row a microsecond away
     *  (2026-11-13, the user's go to the orchestrator's item 2: the checked text was CAST to TIMESTAMP, which keeps the
     *  microsecond alone, so the key silently matched another row or none, and S181's refusal of a lost digit was
     *  bypassed; it is the column type's typed literal now, which DuckDB writes {@code TIMESTAMP_NS '<text>'}). */
    @Test
    void aNanosecondKeyFindsItsRowOnDuckDb() throws Exception {
        Path file = Files.createTempFile("tdg-nanos-", ".duckdb");
        Files.delete(file);
        String nanos = """
                ###Pure
                Class model::S { n: Integer[0..1]; }

                ###Relational
                Database store::NANOS ( Table STAMP ( S TIMESTAMP_NS PRIMARY KEY, N INTEGER ) )

                ###Mapping
                Mapping model::NM (
                    model::S: Relational { ~mainTable [store::NANOS] STAMP n: [store::NANOS] STAMP.N }
                )

                ###Connection
                RelationalDatabaseConnection store::NanosConn {
                    type: DuckDB;
                    specification: DuckDB { path: '%s'; };
                    auth: Test;
                }

                ###Runtime
                Runtime model::NRT {
                    mappings: [ model::NM ];
                    connections: [ store::NANOS: [ c: store::NanosConn ] ];
                }
                """.formatted(file.toString().replace("\\", "/"));
        Seed.sql(nanos, "CREATE TABLE STAMP (S TIMESTAMP_NS PRIMARY KEY, N INTEGER)", "model::NRT");
        Seed.sql(nanos, "INSERT INTO STAMP VALUES (TIMESTAMP_NS '2020-01-02 03:04:05.123456789', 1),"
                + " (TIMESTAMP_NS '2020-01-02 03:04:05.123456', 2)", "model::NRT");
        PureV1Api.Answer one = route("execution/testDataGeneration/generateTestData_WithSeed",
                "{\"clientVersion\":\"vX_X_X\",\"function\":"
                + PureV1Api.grammarToJsonLambda("|model::S.all()->project([d|$d.n], ['n'])", false).json()
                + ",\"model\":" + com.legend.json.Json.toCompact(Map.of("_type", "text", "code", nanos))
                + ",\"mapping\":\"model::NM\",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"model::NRT\"},"
                + "\"context\":{\"_type\":\"BaseExecutionContext\"},\"tableRowIdentifiers\":[{\"table\":{\"_type\":"
                + "\"table\",\"table\":\"STAMP\",\"schema\":\"default\",\"database\":\"store::NANOS\"},"
                + "\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"S\",\"value\":"
                + "\"2020-01-02T03:04:05.123456789+0000\"}]}]}]}");
        assertEquals(200, one.status(), one.json());
        assertTrue(one.json().startsWith("default\nSTAMP\nN,S\n1,"), "the nanosecond key's own row: " + one.json());
        assertEquals(1, one.json().lines().filter(l -> l.matches("\\d+,.*")).count(), "its row alone: " + one.json());
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
        // the tables by schema and table; a table's key, then its scanned columns, sorted (TDG:753-777)
        assertEquals("{\"data\":[{\"_type\":\"relationalCSVData\",\"tables\":[{\"schema\":\"default\",\"table\":"
                + "\"FIRM\",\"values\":\"ID,LEGAL_NAME\"},{\"schema\":\"default\",\"table\":\"PERSON\",\"values\":"
                + "\"ID,FIRM_ID,NAME\"}]}]}", a.json());
    }

    /** The query's parameters bound by name, as upstream binds the request's {@code parameterValues} (register S301's
     *  third part, built: the orchestrator's go of 2026-11-12) -- the seed's keys those the bound query reaches; a
     *  parameter with no value refused with upstream's words; the seed route's own {@code parameters} read by nothing. */
    @Test
    void theQuerysParametersAreBoundByName() {
        String query = "{name: String[1]|model::Person.all()->filter(p|$p.name != $name)->project([p|$p.name],"
                + " ['name'])}";
        String bob = ",\"parameterValues\":[{\"name\":\"name\",\"value\":{\"_type\":\"string\",\"value\":\"Bob\"}}]";
        PureV1Api.Answer seed = route("execution/testDataGeneration/generateSeedData",
                input(query, bob + ",\"parameters\":[\"ignored\"]"));
        assertEquals(200, seed.status(), seed.json());
        assertEquals("{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\",\"database\":"
                + "\"store::DB\"},\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":1}]},"
                + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":3}]},"
                + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":4}]}]}", seed.json());
        PureV1Api.Answer data = route("execution/testDataGeneration/generateTestData_WithDefaultSeed",
                input(query, bob));
        assertEquals(200, data.status(), data.json());
        assertEquals("default\nPERSON\nID,NAME\n1,Ann\n3,Cy\n4,Di\n-----\n", data.json());
        PureV1Api.Answer missing = route("execution/testDataGeneration/generateSeedData", input(query, ""));
        assertEquals(500, missing.status(), missing.json());
        assertTrue(missing.json().contains("Missing external parameter(s): name:String[1]"), missing.json());
    }

    /** A table entry with no {@code rowIdentifiers} field reads as an empty list -- no filter, the table's rows by
     *  key, at most 20 -- as upstream's reader makes it ({@code HelperTestDataGenerationBuilder}: a null list an empty
     *  one; then {@code orFilters([])}). The B2d.3 audit's L3, 2026-11-12: read in the pinned sources, lite already
     *  did so; pinned here. */
    @Test
    void aTableEntryWithNoIdentifiersFieldIsNoFilter() {
        PureV1Api.Answer a = route("execution/testDataGeneration/generateTestData_WithSeed", input(QUERY,
                ",\"tableRowIdentifiers\":[{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\","
                        + "\"database\":\"store::DB\"}}]"));
        assertEquals(200, a.status(), a.json());
        assertEquals("default\nFIRM\nID,LEGAL_NAME\n10,Acme\n20,Bolt\n-----\n"
                + "default\nPERSON\nFIRM_ID,ID,NAME\n10,1,Ann\n10,3,Cy\n20,2,Bob\n---null---,4,Di\n-----\n",
                a.json());
    }

    /** The seed route over a view root: the keys of the view's main table the query reaches through the view, as
     *  upstream's seed plan takes a view root's {@code mainTable()} (TDG:806-808); the test-data routes refuse a view
     *  by name (register S301's draft). The B2d.3 audit's L8, 2026-11-12: the class doc said every route refused. */
    @Test
    void theSeedRouteAdmitsAViewRoot() {
        String view = "|model::PersonView.all()->project([p|$p.name], ['name'])";
        PureV1Api.Answer seed = route("execution/testDataGeneration/generateSeedData", input(view, ""));
        assertEquals(200, seed.status(), seed.json());
        assertEquals("{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\",\"database\":"
                + "\"store::DB\"},\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":1}]},"
                + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":2}]},"
                + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":3}]}]}", seed.json());
        PureV1Api.Answer data = route("execution/testDataGeneration/generateTestData_WithDefaultSeed", input(view, ""));
        assertEquals(500, data.status(), data.json());
        assertTrue(data.json().contains("the view 'PERSON_VIEW' in the query's relation tree on the server's route"),
                data.json());
    }

    /** The seed's query read through the request's mapping whatever mappings its runtime declares, as upstream
     *  executes the seed's function with the given mapping (the B2d.3 audit's L5, 2026-11-12): a runtime declaring
     *  none answers as one declaring it -- until then refused by name. */
    @Test
    void theSeedReadsThroughTheGivenMapping() {
        String query = "|model::Person.all()->filter(p|$p.name != 'Bob')->project([p|$p.name], ['name'])";
        PureV1Api.Answer a = route("execution/testDataGeneration/generateSeedData", input(query, "")
                .replace("\"runtime\":\"model::RT\"", "\"runtime\":\"model::Unmapped\""));
        assertEquals(200, a.status(), a.json());
        assertEquals("{\"table\":{\"_type\":\"table\",\"table\":\"PERSON\",\"schema\":\"default\",\"database\":"
                + "\"store::DB\"},\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":1}]},"
                + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":3}]},"
                + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":4}]}]}", a.json());
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
