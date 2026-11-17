// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.protocol.PureComposer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LITE'S OWN GRAMMAR AND TYPING ROUTES ({@link LiteV1Planning}; the edge program's HOST stream, 2026-11-17), reached as a
 * host reaches them: {@code /api/lite/v1/...} through {@link LiteV1Api#route}. Each answers what the product's own call
 * answers -- the call the tab's lite export makes -- and each request is closed: a key it lacks or does not read is
 * refused by name.
 */
class LiteV1PlanningTest {

    private static final String MODEL = """
            Class demo::Person
            {
              name: String[1];
              age: Integer[1];
            }
            ###Relational
            Database demo::DB ( Table PERSON ( NAME VARCHAR(100), AGE INTEGER ) )
            ###Mapping
            Mapping demo::M
            (
              *demo::Person: Relational
              {
                ~mainTable [demo::DB]PERSON
                name: [demo::DB]PERSON.NAME,
                age: [demo::DB]PERSON.AGE
              }
            )
            """;

    private static final String QUERY = "|demo::Person.all()->project(~[name: p|$p.name, age: p|$p.age])";

    private static LiteV1Api.Answer call(String path, Map<String, Object> request) {
        return LiteV1Api.route(path, null, Json.toCompact(request), (q, r, output) -> {
            throw new AssertionError("nothing is run here");
        });
    }

    private static Map<String, Object> request(Object... pairs) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put((String) pairs[i], pairs[i + 1]);
        }
        return out;
    }

    private static Map<String, Object> textModel() {
        return request("_type", "text", "code", MODEL);
    }

    private static Object lambdaOf(String text) {
        return Json.parse(com.legend.door.Parse.lambdaJson(text, false));
    }

    @Test
    @DisplayName("a model's text as its protocol JSON, the parse door's answer")
    void modelElements() {
        for (boolean sourceInformation : new boolean[] {false, true}) {
            LiteV1Api.Answer a = call(LiteV1Planning.MODEL_ELEMENTS, request("text", MODEL, "sourceInformation", sourceInformation));
            assertEquals(200, a.status(), a.json());
            assertEquals(com.legend.door.Parse.documentJson(MODEL, sourceInformation), a.json());
        }
    }

    @Test
    @DisplayName("a lambda's text as its protocol JSON, and its JSON as its text, as the product reads and prints them")
    void parseAndPrintLambda() {
        LiteV1Api.Answer parsed = call(LiteV1Planning.PARSE_LAMBDA, request("text", QUERY, "sourceInformation", false));
        assertEquals(200, parsed.status(), parsed.json());
        assertEquals(com.legend.door.Parse.lambdaJson(QUERY, false), parsed.json());
        for (String style : new String[] {"PRETTY", "STANDARD"}) {
            LiteV1Api.Answer printed = call(LiteV1Planning.PRINT_LAMBDA, request("lambda", Json.parse(parsed.json()), "style", style));
            assertEquals(200, printed.status(), printed.json());
            assertEquals(PureComposer.lambda(parsed.json(), PureComposer.Style.valueOf(style)),
                    Json.parseObject(printed.json()).getString("text"));
        }
    }

    @Test
    @DisplayName("a lambda's relation type over a model given as text, the compiler's type of its result")
    void lambdaRelationType() {
        LiteV1Api.Answer a = call(LiteV1Planning.LAMBDA_RELATION_TYPE, request("model", textModel(), "lambda", lambdaOf(QUERY)));
        assertEquals(200, a.status(), a.json());
        assertEquals(Json.toCompact(com.legend.plan.RelationTypeJson.of(com.legend.Compiler.query(
                com.legend.Compiler.compileModel(MODEL), com.legend.protocol.ProtocolReader.lambda(
                        com.legend.door.Parse.lambdaJson(QUERY, false))).resultType())), a.json());
        assertTrue(a.json().contains("\"name\"") && a.json().contains("age"), a.json());
    }

    @Test
    @DisplayName("text that does not parse or compile is refused with its kind: PARSER, COMPILATION")
    void refusalsWithTheirKind() {
        LiteV1Api.Answer badModel = call(LiteV1Planning.MODEL_ELEMENTS, request("text", "Class demo::{", "sourceInformation", false));
        assertEquals(400, badModel.status(), badModel.json());
        assertEquals("PARSER", Json.parseObject(badModel.json()).getString("errorType"));
        LiteV1Api.Answer badLambda = call(LiteV1Planning.PARSE_LAMBDA, request("text", "|demo::Person.all()->", "sourceInformation", false));
        assertEquals(400, badLambda.status(), badLambda.json());
        assertEquals("PARSER", Json.parseObject(badLambda.json()).getString("errorType"));
        LiteV1Api.Answer badType = call(LiteV1Planning.LAMBDA_RELATION_TYPE,
                request("model", textModel(), "lambda", lambdaOf("|demo::Person.all()->project(~[x: p|$p.nope])")));
        assertEquals(400, badType.status(), badType.json());
        assertEquals("COMPILATION", Json.parseObject(badType.json()).getString("errorType"));
    }

    @Test
    @DisplayName("each request is closed: a key it lacks, a key it does not read, and a style it does not print, refused by name")
    void closedRequests() {
        LiteV1Api.Answer extra = call(LiteV1Planning.PARSE_LAMBDA, request("text", QUERY, "sourceInformation", false, "returnSourceInformation", false));
        assertEquals(500, extra.status(), extra.json());
        assertTrue(Json.parseObject(extra.json()).getString("message").contains("a parseLambda request has no key returnSourceInformation"), extra.json());
        LiteV1Api.Answer missing = call(LiteV1Planning.MODEL_ELEMENTS, request("text", MODEL));
        assertEquals(500, missing.status(), missing.json());
        assertTrue(Json.parseObject(missing.json()).getString("message").contains("a modelElements request names no sourceInformation"), missing.json());
        LiteV1Api.Answer style = call(LiteV1Planning.PRINT_LAMBDA, request("lambda", lambdaOf(QUERY), "style", "COMPACT"));
        assertEquals(500, style.status(), style.json());
        assertTrue(Json.parseObject(style.json()).getString("message").contains("a lambda prints PRETTY or STANDARD, not 'COMPACT'"), style.json());
    }

    @Test
    @DisplayName("a path under lite's API that is none of its routes is still LiteV1Api's 404")
    void anyOtherPath() {
        LiteV1Api.Answer a = call("/api/lite/v1/grammar/lambdaText", request());
        assertEquals(404, a.status(), a.json());
    }

    // ---- a query's plan and its run (the edge program's H7, 2026-11-17) ----------------------------------------------

    /** A project with its own Data rows, on an in-memory DuckDB the server makes holding them (ProjectRowsTest's). */
    private static final String PARTY = """
            Class proj::Party { id: Integer[1]; name: String[1]; joined: StrictDate[1]; }
            ###Relational
            Database proj::PartyDB ( Schema PARTY ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), JOINED DATE) ) )
            ###Mapping
            Mapping proj::PartyMapping
            (
              *proj::Party: Relational
              {
                ~primaryKey ([proj::PartyDB] PARTY.PARTY.ID)
                ~mainTable [proj::PartyDB] PARTY.PARTY
                id: [proj::PartyDB] PARTY.PARTY.ID,
                name: [proj::PartyDB] PARTY.PARTY.NAME,
                joined: [proj::PartyDB] PARTY.PARTY.JOINED
              }
            )
            ###Connection
            RelationalDatabaseConnection proj::Conn { store: proj::PartyDB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime proj::RT { mappings: [ proj::PartyMapping ]; connections: [ proj::PartyDB: [ db: proj::Conn ] ]; }
            ###Data
            Data proj::PartyData
            {
              Relational
              #{
                PARTY.PARTY:
                  'ID,NAME,JOINED\\n1,Meridian Capital,2021-03-04\\n2,Halberd Securities,2022-05-06\\n';
              }#
            }
            """;

    /** Parties from a date on, each its id, name and join date. */
    private static final String SINCE = "{since: StrictDate[1]|proj::Party.all()->filter(p|$p.joined >= $since)"
            + "->project(~[id: p|$p.id, name: p|$p.name, joined: p|$p.joined])->sort(~id->ascending())"
            + "->from(proj::PartyMapping, proj::RT)}";

    private static LiteV1Api.Answer run(String path, Map<String, Object> request) {
        return LiteV1Planning.execution("POST", path, Json.toCompact(request), ConnectionResolver.liteRunner(null));
    }

    private static Map<String, Object> party() {
        return request("_type", "text", "code", PARTY);
    }

    @Test
    @DisplayName("execution/plan: the lambda's plan, planned and not run -- the plan the execute route runs")
    void executionPlan() {
        LiteV1Api.Answer a = run(LiteV1Planning.EXECUTION_PLAN, request("model", party(), "lambda", lambdaOf(SINCE)));
        assertEquals(200, a.status(), a.json());
        assertEquals(com.legend.executionplan.PlanJson.write(com.legend.Compiler.query(com.legend.Compiler.compileModel(
                PARTY), com.legend.protocol.ProtocolReader.lambda(com.legend.door.Parse.lambdaJson(SINCE, false)))
                .projectPlan(null)), a.json());
        // the parameter is the plan's, bound when it runs: declared, never written into the statement
        assertTrue(a.json().contains("\"since\""), a.json());
        assertTrue(!a.json().contains("2022-01-01"), a.json());
    }

    @Test
    @DisplayName("execution/execute: the plan run with its parameter bound at run, answered as lite's typed rows")
    void execute() {
        LiteV1Api.Answer a = run(LiteV1Planning.EXECUTION_EXECUTE, request("model", party(), "lambda", lambdaOf(SINCE),
                "parameters", request("since", "2022-01-01")));
        assertEquals(200, a.status(), a.json());
        Json.Obj answer = Json.parseObject(a.json());
        assertEquals("RELATION", answer.getString("shape"));
        assertEquals("[{\"name\":\"id\",\"type\":\"Integer\"},{\"name\":\"name\",\"type\":\"String\"},"
                + "{\"name\":\"joined\",\"type\":\"StrictDate\"}]", Json.toCompact(answer.get("columns")));
        // one party from 2022 on: an integer's digits, a text, a date's text
        assertEquals("[[2,\"Halberd Securities\",\"2022-05-06\"]]", Json.toCompact(answer.get("rows")));
        assertTrue(answer.getString("sql").contains("?"), answer.getString("sql"));
        // another value, the same plan: both parties
        LiteV1Api.Answer both = run(LiteV1Planning.EXECUTION_EXECUTE, request("model", party(), "lambda",
                lambdaOf(SINCE), "parameters", request("since", "2020-01-01")));
        assertEquals(200, both.status(), both.json());
        assertEquals(2, Json.parseObject(both.json()).getArr("rows").items().size(), both.json());
    }

    @Test
    @DisplayName("execution/execute refuses by name: a parameter with no value, a value of the wrong JSON kind")
    void executeRefusals() {
        LiteV1Api.Answer missing = run(LiteV1Planning.EXECUTION_EXECUTE, request("model", party(), "lambda",
                lambdaOf(SINCE), "parameters", request()));
        assertEquals(500, missing.status(), missing.json());
        assertTrue(Json.parseObject(missing.json()).getString("message")
                .contains("no value for the parameter(s) since:StrictDate[1]"), missing.json());
        LiteV1Api.Answer wrong = run(LiteV1Planning.EXECUTION_EXECUTE, request("model", party(), "lambda",
                lambdaOf(SINCE), "parameters", request("since", 20220101)));
        assertEquals(500, wrong.status(), wrong.json());
        assertTrue(Json.parseObject(wrong.json()).getString("message")
                .contains("parameter 'since' (StrictDate): given 20220101, which is no StrictDate's JSON value"), wrong.json());
        LiteV1Api.Answer notADate = run(LiteV1Planning.EXECUTION_EXECUTE, request("model", party(), "lambda",
                lambdaOf(SINCE), "parameters", request("since", "2022-13-01")));
        assertEquals(500, notADate.status(), notADate.json());
        assertTrue(Json.parseObject(notADate.json()).getString("message")
                .contains("parameter 'since': given \"2022-13-01\", which is no StrictDate's text"), notADate.json());
    }

    @Test
    @DisplayName("the execution routes are closed and POST only: an unread key, a missing one, a GET, another path")
    void executionRequestsClosed() {
        LiteV1Api.Answer extra = run(LiteV1Planning.EXECUTION_EXECUTE, request("model", party(), "lambda",
                lambdaOf(SINCE), "parameters", request(), "parameterValues", java.util.List.of()));
        assertEquals(500, extra.status(), extra.json());
        assertTrue(Json.parseObject(extra.json()).getString("message").contains(
                "an execute request has no key parameterValues (it holds model, lambda, parameters; optional runtime)"),
                extra.json());
        LiteV1Api.Answer noParameters = run(LiteV1Planning.EXECUTION_EXECUTE, request("model", party(), "lambda",
                lambdaOf(SINCE)));
        assertEquals(500, noParameters.status(), noParameters.json());
        assertTrue(Json.parseObject(noParameters.json()).getString("message").contains(
                "an execute request names no parameters"), noParameters.json());
        assertEquals(405, LiteV1Planning.execution("GET", LiteV1Planning.EXECUTION_PLAN, "",
                ConnectionResolver.liteRunner(null)).status());
        assertEquals(404, LiteV1Planning.execution("POST", LiteV1Planning.EXECUTION + "/run", "{}",
                ConnectionResolver.liteRunner(null)).status());
    }

    @Test
    @DisplayName("lite's server mounts the execution routes: execution/execute over HTTP answers the same rows")
    void theServerMountsThem() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0);
        server.start();
        try {
            java.net.http.HttpResponse<String> r = java.net.http.HttpClient.newHttpClient().send(
                    java.net.http.HttpRequest.newBuilder()
                            .uri(java.net.URI.create("http://localhost:" + server.getPort()
                                    + LiteV1Planning.EXECUTION_EXECUTE))
                            .POST(java.net.http.HttpRequest.BodyPublishers.ofString(Json.toCompact(request("model",
                                    party(), "lambda", lambdaOf(SINCE), "parameters", request("since", "2022-01-01")))))
                            .build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals(200, r.statusCode(), r.body());
            assertEquals("[[2,\"Halberd Securities\",\"2022-05-06\"]]",
                    Json.toCompact(Json.parseObject(r.body()).get("rows")));
        } finally {
            server.stop();
        }
    }
}
