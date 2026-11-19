// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.protocol.PureComposer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * LITE'S OWN API, v1, THE SERVER BINDING (lite's API v1 spec, docs/records/lite-api-v1-2026-11-17.md, §2-§5; the user's
 * go of 2026-11-17): each operation reached as a host reaches it, {@code /api/lite/v1/<operation>} through
 * {@link LiteV1Api#route} -- the model's ({@code model/check}, {@code model/elements}) and the query's
 * ({@code query/parse}, {@code query/print}, {@code query/type}, {@code query/plan}, {@code query/run}) -- each answering
 * what the product's own call answers, each request closed, and every refusal lite's one shape, {@code {kind, message,
 * where?, raise?}}, its status by its kind.
 */
class LiteV1PlanningTest {

    private static final String MODEL = """
            Enum demo::Tier { GOLD, SILVER }
            Class demo::Person
            {
              name: String[1];
              age: Integer[1];
              tier: demo::Tier[1];
            }
            ###Relational
            Database demo::DB ( Table PERSON ( NAME VARCHAR(100), AGE INTEGER, TIER VARCHAR(10) ) )
            ###Mapping
            Mapping demo::M
            (
              demo::Tier: EnumerationMapping TierMap
              {
                GOLD: 'G',
                SILVER: 'S'
              }
              *demo::Person: Relational
              {
                ~mainTable [demo::DB]PERSON
                name: [demo::DB]PERSON.NAME,
                age: [demo::DB]PERSON.AGE,
                tier: EnumerationMapping TierMap: [demo::DB]PERSON.TIER
              }
            )
            """;

    private static final String QUERY = "|demo::Person.all()->project(~[name: p|$p.name, age: p|$p.age])";

    /** The operation's answer, run nothing. */
    private static LiteV1Api.Answer call(String operation, Map<String, Object> request) {
        return LiteV1Api.route(LiteV1Planning.API + operation, Json.toCompact(request), (q, r, values, output) -> {
            throw new AssertionError("nothing is run here");
        }, LiteV1Api.Models.COMPILED);
    }

    private static Map<String, Object> request(Object... pairs) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put((String) pairs[i], pairs[i + 1]);
        }
        return out;
    }

    private static Object lambdaOf(String text) {
        return Json.parse(com.legend.door.Parse.lambdaJson(text, false));
    }

    /** A refusal's kind, its status the kind's, and its message holding {@code words}. */
    private static Json.Obj refused(LiteV1Api.Answer a, String kind, int status, String words) {
        assertEquals(status, a.status(), a.json());
        Json.Obj error = Json.parseObject(a.json());
        assertEquals(kind, error.getString("kind"), a.json());
        assertTrue(error.getString("message").contains(words), a.json());
        // lite's shape alone: none of legend-engine's envelope (spec §3)
        assertFalse(error.has("code") || error.has("status") || error.has("errorType"), a.json());
        return error;
    }

    @Test
    @DisplayName("model/elements: the model's protocol elements, the parse door's")
    void modelElements() {
        LiteV1Api.Answer a = call("model/elements", request("model", MODEL));
        assertEquals(200, a.status(), a.json());
        assertEquals(Json.toCompact(Json.parseObject(com.legend.door.Parse.documentJson(MODEL, false)).get("elements")),
                Json.toCompact(Json.parseObject(a.json()).get("elements")));
    }

    @Test
    @DisplayName("model/check: every error in lite's shape, none when it compiles")
    void modelCheck() {
        assertEquals("{\"errors\":[]}", call("model/check", request("model", MODEL)).json());
        LiteV1Api.Answer a = call("model/check", request("model", "Class demo::Desk\n{\n  name: Strin[1];\n}\n"));
        assertEquals(200, a.status(), a.json());
        List<Json.Node> errors = Json.parseObject(a.json()).getArr("errors").items();
        assertEquals(1, errors.size(), a.json());
        Json.Obj e = (Json.Obj) errors.get(0);
        assertEquals("compile", e.getString("kind"), a.json());
        assertTrue(e.getString("message").contains("Strin"), a.json());
        assertTrue(e.has("where"), a.json());
    }

    @Test
    @DisplayName("query/parse and query/print: a query's text as its tree and back, pretty unless standard is asked")
    void parseAndPrint() {
        LiteV1Api.Answer parsed = call("query/parse", request("text", QUERY));
        assertEquals(200, parsed.status(), parsed.json());
        Json.Node query = Json.parseObject(parsed.json()).get("query");
        assertEquals(com.legend.door.Parse.lambdaJson(QUERY, false), Json.toCompact(query));
        String tree = Json.toCompact(query);
        assertEquals(PureComposer.lambda(tree, PureComposer.Style.PRETTY),
                Json.parseObject(call("query/print", request("query", query)).json()).getString("text"));
        assertEquals(PureComposer.lambda(tree, PureComposer.Style.PRETTY),
                Json.parseObject(call("query/print", request("query", query, "style", "pretty")).json()).getString("text"));
        assertEquals(PureComposer.lambda(tree, PureComposer.Style.STANDARD), Json.parseObject(call("query/print",
                request("query", query, "style", "standard")).json()).getString("text"));
    }

    @Test
    @DisplayName("query/parse keeps a number exact: an Integer past a double's 2^53, a Decimal's digits")
    void parseKeepsNumbersExact() {
        String text = "|9223372036854775807 + 0.1000000000000000000001D->toOne()->floor()";
        LiteV1Api.Answer parsed = call("query/parse", request("text", text));
        assertEquals(200, parsed.status(), parsed.json());
        assertTrue(parsed.json().contains("9223372036854775807"), parsed.json());
        assertTrue(parsed.json().contains("0.1000000000000000000001"), parsed.json());
    }

    @Test
    @DisplayName("query/parse reads lite's own grammar: an island (#Cypher{...}#) parses, as legend-engine's route does not")
    void parseReadsLitesGrammar() {
        String text = "|#Cypher{MATCH (p:Person) WHERE p.age > 30 RETURN p.name}#";
        LiteV1Api.Answer parsed = call("query/parse", request("text", text));
        assertEquals(200, parsed.status(), parsed.json());
        assertTrue(parsed.json().contains("MATCH (p:Person)"), parsed.json());
        // legend-engine's grammarToJson, the edge's, reads legend-engine's grammar: no island of lite's
        // legend-engine's grammar refuses it (its entry is the edge's door alone, J21: the parser asked here directly)
        assertThrows(RuntimeException.class, () -> com.legend.parser.SpecParser.lambdaJson(text, false));
    }

    @Test
    @DisplayName("query/type: each column's name and Pure type; an enumeration's says so")
    void queryType() {
        LiteV1Api.Answer a = call("query/type", request("model", MODEL, "query",
                lambdaOf("|demo::Person.all()->project(~[name: p|$p.name, age: p|$p.age, tier: p|$p.tier])")));
        assertEquals(200, a.status(), a.json());
        assertEquals("{\"columns\":[{\"name\":\"name\",\"type\":\"String\"},{\"name\":\"age\",\"type\":\"Integer\"},"
                + "{\"name\":\"tier\",\"type\":\"demo::Tier\",\"enumeration\":true}]}", a.json());
    }

    @Test
    @DisplayName("refusals carry their kind and the kind's status: parse, compile, request")
    void refusalsWithTheirKind() {
        Json.Obj parse = refused(call("query/parse", request("text", "|demo::Person.all()->")), "parse", 400, "");
        assertTrue(parse.has("where"), Json.toCompact(parse));
        refused(call("model/elements", request("model", "Class demo::{")), "parse", 400, "");
        refused(call("query/type", request("model", MODEL, "query",
                lambdaOf("|demo::Person.all()->project(~[x: p|$p.nope])"))), "compile", 400, "nope");
        refused(call("query/print", request("query", lambdaOf(QUERY), "style", "COMPACT")), "request", 400,
                "a query prints in the style pretty or standard, not 'COMPACT'");
    }

    @Test
    @DisplayName("each request is closed: a key it lacks or does not read, a value of the wrong JSON kind, refused by name")
    void closedRequests() {
        refused(call("query/parse", request("text", QUERY, "sourceInformation", false)), "request", 400,
                "a query/parse request has no key sourceInformation (it holds text)");
        refused(call("query/type", request("model", MODEL)), "request", 400,
                "a query/type request names no query (it holds model, query)");
        refused(call("model/elements", request("model", request("_type", "text", "code", MODEL))), "request", 400,
                "the request's model is not a string");
        refused(LiteV1Api.route(LiteV1Planning.API + "query/parse", "[1]", (q, r, v, o) -> {
            throw new AssertionError();
        }, LiteV1Api.Models.COMPILED), "request", 400, "a query/parse request is not a JSON object");
        // a model action's request lacking a key it needs (until 2026-11-17 the JSON reader's "Missing required field",
        // a defect's 500)
        refused(call("model/createClass", request()), "request", 400, "a createClass request names no project");
        refused(call("model/createDataSpace", request("project", request(), "package", "p")), "request", 400,
                "a createDataSpace request names no dataSpace");
    }

    @Test
    @DisplayName("a path that names no operation is refused by name, kind request; the old names answer no more")
    void anyOtherPath() {
        for (String old : List.of("grammar/parseLambda", "grammar/printLambda", "grammar/modelElements",
                "compilation/lambdaRelationType", "compilation/compile", "execution/plan", "execution/execute",
                "grammar/tsItem", "grammar/jsonToTs/lambda")) {
            // an unknown route, 404 notFound (the orchestrator's ruling of 2026-11-19: until then request, 400)
            refused(call(old, request()), "notFound", 404, "no such legend-lite API: /api/lite/v1/" + old);
        }
    }

    // ---- a query's plan and its run ------------------------------------------------------------------------------------

    /** A project with its own Data rows, on an in-memory DuckDB the server makes holding them (ProjectRowsTest's). */
    private static final String PARTY = """
            Class proj::Party { id: Integer[1]; name: String[1]; joined: StrictDate[1]; }
            ###Relational
            Database proj::PartyDB ( Schema PARTY ( Table PARTY (ID BIGINT PRIMARY KEY, NAME VARCHAR(200), JOINED DATE) ) )
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
                  'ID,NAME,JOINED\\n1,Meridian Capital,2021-03-04\\n9223372036854775807,Halberd Securities,2022-05-06\\n';
              }#
            }
            """;

    private static final String RUNTIME = "proj::RT";

    /** Parties from a date on, each its id, name and join date. */
    private static final String SINCE = "{since: StrictDate[1]|proj::Party.all()->filter(p|$p.joined >= $since)"
            + "->project(~[id: p|$p.id, name: p|$p.name, joined: p|$p.joined])->sort(~id->ascending())"
            + "->from(proj::PartyMapping, proj::RT)}";

    /** Parties up to an id. */
    private static final String UP_TO = "{n: Integer[1]|proj::Party.all()->filter(p|$p.id <= $n)"
            + "->project(~[id: p|$p.id])->sort(~id->ascending())->from(proj::PartyMapping, proj::RT)}";

    /** The operation's answer, its run the server's product door over its project databases. */
    private static LiteV1Api.Answer run(String operation, Map<String, Object> request) {
        return LiteV1Api.route(LiteV1Planning.API + operation, Json.toCompact(request),
                ConnectionResolver.liteRunner(null), LiteV1Api.Models.COMPILED);
    }

    @Test
    @DisplayName("query/plan: lite's plan, data, planned and not run -- the plan query/run runs; the runtime required")
    void queryPlan() {
        LiteV1Api.Answer a = run("query/plan", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME));
        assertEquals(200, a.status(), a.json());
        assertEquals(com.legend.executionplan.PlanJson.write(com.legend.Compiler.query(com.legend.Compiler.compileModel(
                PARTY), com.legend.protocol.ProtocolReader.lambda(com.legend.door.Parse.lambdaJson(SINCE, false)))
                .projectPlan(RUNTIME)), Json.toCompact(Json.parseObject(a.json()).get("plan")));
        // the parameter is the plan's, bound when it runs: declared, never written into the statement
        assertTrue(a.json().contains("\"since\""), a.json());
        assertFalse(a.json().contains("2022-01-01"), a.json());
        refused(run("query/plan", request("model", PARTY, "query", lambdaOf(SINCE))), "request", 400,
                "a query/plan request names no runtime");
    }

    @Test
    @DisplayName("query/run: the plan run with its parameter bound at run, answered as lite's typed rows")
    void queryRun() {
        LiteV1Api.Answer a = run("query/run", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "format", "json", "params", request("since", "2022-01-01")));
        assertEquals(200, a.status(), a.json());
        Json.Obj answer = Json.parseObject(a.json());
        assertEquals("RELATION", answer.getString("shape"));
        assertEquals("[{\"name\":\"id\",\"type\":\"Integer\"},{\"name\":\"name\",\"type\":\"String\"},"
                + "{\"name\":\"joined\",\"type\":\"StrictDate\"}]", Json.toCompact(answer.get("columns")));
        // one party from 2022 on: an Integer an exact JSON number (2^63-1, spec §4), a text, a date's text
        assertEquals("[[9223372036854775807,\"Halberd Securities\",\"2022-05-06\"]]", Json.toCompact(answer.get("rows")));
        assertFalse(answer.has("sql"), a.json());
        // another value, the same plan: both parties
        LiteV1Api.Answer both = run("query/run", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "params", request("since", "2020-01-01"), "format", "json"));
        assertEquals(200, both.status(), both.json());
        assertEquals(2, Json.parseObject(both.json()).getArr("rows").items().size(), both.json());
    }

    @Test
    @DisplayName("query/run binds an Integer parameter in the 64-bit range, exact: 2^63-1 itself")
    void anIntegerParameterTakesThe64BitRange() {
        LiteV1Api.Answer max = run("query/run", request("model", PARTY, "query", lambdaOf(UP_TO), "runtime", RUNTIME,
                "format", "json", "params", Json.parseObject("{\"n\":9223372036854775807}")));
        assertEquals(200, max.status(), max.json());
        assertEquals("[[1],[9223372036854775807]]", Json.toCompact(Json.parseObject(max.json()).get("rows")));
        LiteV1Api.Answer below = run("query/run", request("model", PARTY, "query", lambdaOf(UP_TO), "runtime", RUNTIME,
                "format", "json", "params", Json.parseObject("{\"n\":9223372036854775806}")));
        assertEquals("[[1]]", Json.toCompact(Json.parseObject(below.json()).get("rows")), below.json());
        refused(run("query/run", request("model", PARTY, "query", lambdaOf(UP_TO), "runtime", RUNTIME,
                "format", "json", "params", Json.parseObject("{\"n\":9223372036854775808}"))), "parameters", 400,
                "parameter 'n' (Integer): given 9223372036854775808, which is beyond the 64-bit range an Integer holds");
        refused(run("query/run", request("model", PARTY, "query", lambdaOf(UP_TO), "runtime", RUNTIME,
                "format", "json", "params", Json.parseObject("{\"n\":1.5}"))), "parameters", 400,
                "parameter 'n' (Integer): given 1.5, which is no Integer's JSON value");
    }

    @Test
    @DisplayName("query/run in Arrow: the same rows as one Arrow IPC stream of typed columns, by the caller's ask")
    void queryRunInArrow() {
        LiteV1Api.Answer a = run("query/run", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "params", request("since", "2020-01-01"), "format", "arrow"));
        assertEquals(200, a.status(), a.json());
        assertEquals(LiteV1Planning.ARROW, a.contentType());
        byte[] bytes = a.body();
        // an IPC stream: each message after the continuation marker 0xFFFFFFFF
        assertEquals(-1, java.nio.ByteBuffer.wrap(bytes, 0, 4).getInt(), "the continuation marker");
        String latin1 = new String(bytes, StandardCharsets.ISO_8859_1);
        assertTrue(latin1.contains("Halberd Securities") && latin1.contains("Meridian Capital"), "both rows' text");
        assertTrue(latin1.contains("joined"), "the columns by name");
        // the answer's shape and each column's Pure type ride as metadata (what the JSON answer says as shape, columns)
        assertTrue(latin1.contains("legend.shape") && latin1.contains("RELATION"), "the shape");
        assertTrue(latin1.contains("legend.type") && latin1.contains("StrictDate"), "the columns' types");
        refused(run("query/run", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "params", request("since", "2020-01-01"), "format", "csv")), "request", 400,
                "a query runs in the format json or arrow, not 'csv'");
    }

    @Test
    @DisplayName("query/run refuses the caller's values by name, kind parameters: one missing, one of the wrong kind")
    void queryRunRefusesParameters() {
        refused(run("query/run", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "format", "json")), "parameters", 400, "no value for the parameter(s) since:StrictDate[1]");
        refused(run("query/run", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "format", "json", "params", request("since", 20220101))), "parameters", 400,
                "parameter 'since' (StrictDate): given 20220101, which is no StrictDate's JSON value");
        refused(run("query/run", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "format", "json", "params", request("since", "2022-13-01"))), "parameters", 400,
                "parameter 'since': given \"2022-13-01\", which is no StrictDate's text");
        refused(run("query/run", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "format", "json", "params", request(), "parameterValues", List.of())), "request", 400,
                "a query/run request has no key parameterValues (it holds model, query, runtime, format; optional params)");
    }

    @Test
    @DisplayName("query/run's format is the caller's ask, never a guess: a request without it refused by name, kind request")
    void queryRunRequiresItsFormat() {
        // spec §4 (the orchestrator's ruling of 2026-11-17, G7): until then a missing format answered json
        refused(run("query/run", request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "params", request("since", "2022-01-01"))), "request", 400,
                "a query/run request names no format (it holds model, query, runtime, format; optional params)");
    }

    @Test
    @DisplayName("query/run's raise is the data's: kind data, 422, its kind named in raise")
    void aRaiseIsTheDatas() {
        Json.Obj error = refused(run("query/run", request("model", PARTY, "query",
                lambdaOf("|proj::Party.all()->project(~[x: p|$p.id->rem(0)])->from(proj::PartyMapping, proj::RT)"),
                "runtime", RUNTIME, "format", "json")), "data", 422, "");
        assertTrue(error.has("raise"), Json.toCompact(error));
    }

    @Test
    @DisplayName("query/run's request read for a host that runs it itself: the server's reading, its words its refusals")
    void queryRunRequestIsTheServersReading() {
        // H4's P4 (the user's CP-13, 2026-11-19): Python reads its query/run request by this, never by words of its own
        String asked = Json.toCompact(request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "format", "arrow", "params", request("since", "2022-01-01")));
        Json.Obj read = Json.parseObject(LiteV1Planning.queryRunRequestJson(asked));
        assertEquals(List.of("model", "query", "runtime", "format", "params"), List.copyOf(read.fields().keySet()));
        assertEquals(PARTY, read.getString("model"));
        assertEquals(Json.toCompact(lambdaOf(SINCE)), Json.toCompact(read.get("query")));
        assertEquals("arrow", read.getString("format"));
        assertEquals("{\"since\":\"2022-01-01\"}", Json.toCompact(read.get("params")));
        // no params: an empty object, as the server reads none
        assertEquals("{}", Json.toCompact(Json.parseObject(LiteV1Planning.queryRunRequestJson(Json.toCompact(request(
                "model", PARTY, "query", lambdaOf(UP_TO), "runtime", RUNTIME, "format", "json")))).get("params")));
        // each refusal the route's own, word for word
        for (Map<String, Object> wrong : List.of(
                request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME),
                request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME, "format", "csv"),
                request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME, "format", "json", "extra", 1),
                request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME, "format", "json",
                        "params", request("since", 20220101)))) {
            LiteV1Api.Answer route = run("query/run", wrong);
            com.legend.lite.LiteWire.Refusal refusal = LiteV1Api.refusal(assertThrows(RuntimeException.class,
                    () -> LiteV1Planning.queryRunRequestJson(Json.toCompact(wrong))));
            assertEquals(route.status(), refusal.kind().status(), route.json());
            assertEquals(route.json(), refusal.json());
        }
    }

    @Test
    @DisplayName("query/run's answer from a host's own rows is the server's text: JSON by its writer, Arrow by its facts")
    void aHostsRowsAnswerAsTheServerWritesThem() {
        // H4's P5 (the user's CP-13, 2026-11-19): Python's answer, until then written by rules of its own (_lite_cell)
        Map<String, Object> asked = request("model", PARTY, "query", lambdaOf(SINCE), "runtime", RUNTIME,
                "format", "json", "params", request("since", "2020-01-01"));
        LiteV1Api.Answer server = run("query/run", asked);
        assertEquals(200, server.status(), server.json());
        String plan = com.legend.executionplan.PlanJson.write(com.legend.Compiler.query(com.legend.Compiler.compileModel(
                PARTY), com.legend.protocol.ProtocolReader.lambda(com.legend.door.Parse.lambdaJson(SINCE, false)))
                .executionPlan(RUNTIME));
        // the host's rows as its driver read them (result.HostRows' wire): a whole number's digits, a date's text
        String rows = "[{\"id\":1,\"name\":\"Meridian Capital\",\"joined\":\"2021-03-04\"},"
                + "{\"id\":9223372036854775807,\"name\":\"Halberd Securities\",\"joined\":\"2022-05-06\"}]";
        assertEquals(server.json(), LiteV1Planning.hostRunJson(plan, rows));
        // Arrow: the stream's shape and each field's facts, the server's writer's (result.ArrowWriter.facts)
        assertEquals("{\"schema\":{\"legend.shape\":\"RELATION\"},\"fields\":["
                + "{\"name\":\"id\",\"metadata\":{\"legend.type\":\"Integer\",\"legend.multiplicity\":\"[1]\","
                + "\"legend.slot\":\"BIGINT\"}},"
                + "{\"name\":\"name\",\"metadata\":{\"legend.type\":\"String\",\"legend.multiplicity\":\"[1]\","
                + "\"legend.slot\":\"VARCHAR\"}},"
                + "{\"name\":\"joined\",\"metadata\":{\"legend.type\":\"StrictDate\",\"legend.multiplicity\":\"[1]\","
                + "\"legend.slot\":\"DATE\",\"legend.precision\":\"DAY\"}}]}", LiteV1Planning.hostRunArrowFacts(plan));
        // a plan whose root is no one statement's typed result is refused by name
        assertThrows(IllegalArgumentException.class, () -> LiteV1Planning.hostRunJson("{}", "[]"));
    }

    @Test
    @DisplayName("user and a call by another method than POST: one answer each, the words every binding says")
    void userAndNotPostedAreOneAnswerEach() {
        // H4's P4 (the user's CP-13, 2026-11-19): Python's and the server's routes say these, never words of their own
        assertEquals("{\"user\":\"someone\"}", LiteV1Api.user("POST", com.legend.lite.LiteWire.USER, "", "someone").json());
        assertEquals("{\"user\":\"someone\"}", LiteV1Api.user("POST", com.legend.lite.LiteWire.USER, " {} ", "someone").json());
        refused(LiteV1Api.user("POST", com.legend.lite.LiteWire.USER, "{\"x\":1}", "someone"), "request", 400,
                "a user request has no key x (it holds none)");
        LiteV1Api.Answer getUser = LiteV1Api.user("GET", com.legend.lite.LiteWire.USER, "", "someone");
        refused(getUser, "methodNotAllowed", 405, "lite's user operation takes POST, not GET: /api/lite/v1/user");
        assertEquals(Map.of("Allow", "POST"), getUser.headers());
        refused(LiteV1Api.user("POST", com.legend.lite.LiteWire.USER + "/x", "", "someone"), "notFound", 404,
                "no such legend-lite API: /api/lite/v1/user/x");
        LiteV1Api.Answer put = LiteV1Api.notPosted("PUT", LiteV1Planning.API + "model/check");
        refused(put, "methodNotAllowed", 405, "lite's operations take POST, not PUT: /api/lite/v1/model/check");
        assertEquals(Map.of("Allow", "POST"), put.headers());
        refused(LiteV1Api.notPosted("GET", LiteV1Planning.API + "model/nothing"), "notFound", 404,
                "no such legend-lite API: /api/lite/v1/model/nothing");
        // the known routes are the one road's: each answered by it, an unknown one refused notFound (the orchestrator's
        // ruling of 2026-11-19: the set held equal to the switch; the census holds the switch to LITE_ROUTES)
        for (String route : com.legend.lite.LiteWire.ROUTES) {
            if (route.equals(com.legend.lite.LiteWire.USER) || route.equals(LiteV1Planning.API + "query/run")) {
                continue;
            }
            assertFalse(Json.parseObject(LiteV1Api.answer(() -> LiteV1Api.operate(route, "{}", LiteV1Api.Models.COMPILED,
                    com.legend.TypedQuery::projectPlan)).json()).getStringOr("kind", "").equals("notFound"), route);
        }
    }

    @Test
    @DisplayName("lite's server mounts its operations: query/run over HTTP; a GET refused; user from the store")
    void theServerMountsThem() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0);
        server.start();
        try {
            java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
            String base = "http://localhost:" + server.getPort() + LiteV1Planning.API;
            java.net.http.HttpResponse<String> r = client.send(java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(base + "query/run"))
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(Json.toCompact(request("model", PARTY,
                            "query", lambdaOf(SINCE), "runtime", RUNTIME, "format", "json",
                            "params", request("since", "2022-01-01")))))
                    .build(), java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals(200, r.statusCode(), r.body());
            assertEquals("[[9223372036854775807,\"Halberd Securities\",\"2022-05-06\"]]",
                    Json.toCompact(Json.parseObject(r.body()).get("rows")));
            java.net.http.HttpResponse<String> get = client.send(java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(base + "query/run")).GET().build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            // a known route by another method: 405, its Allow (the orchestrator's ruling of 2026-11-19: until then 400)
            assertEquals(405, get.statusCode(), get.body());
            assertEquals("{\"kind\":\"methodNotAllowed\",\"message\":\"lite's operations take POST, not GET: "
                    + LiteV1Planning.API + "query/run\"}", get.body());
            assertEquals(java.util.Optional.of("POST"), get.headers().firstValue("Allow"));
            // an unknown route by any method: 404, the path read first
            java.net.http.HttpResponse<String> nowhere = client.send(java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(base + "query/nowhere")).GET().build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals(404, nowhere.statusCode(), nowhere.body());
            assertEquals("{\"kind\":\"notFound\",\"message\":\"no such legend-lite API: " + LiteV1Planning.API
                    + "query/nowhere\"}", nowhere.body());
            java.net.http.HttpResponse<String> getUser = client.send(java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(base + "user")).GET().build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals(405, getUser.statusCode(), getUser.body());
            assertEquals(java.util.Optional.of("POST"), getUser.headers().firstValue("Allow"));
            java.net.http.HttpResponse<String> user = client.send(java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(base + "user"))
                    .POST(java.net.http.HttpRequest.BodyPublishers.noBody()).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals(200, user.statusCode(), user.body());
            assertEquals("{\"user\":\"anonymous\"}", user.body());
        } finally {
            server.stop();
        }
    }
}
