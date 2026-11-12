package com.legend.server;

import com.legend.json.Json;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * legend-engine's {@code pure/v1} API through the REAL HTTP server
 * (docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md). Carries what {@code /engine/plan}'s test
 * pinned before that made-up endpoint was deleted (2026-09-27): planning never opens the
 * database (the model's connection names a file that does not exist), an honest compile
 * error is an error answer, and only POST is served.
 */
class PureV1HttpTest {

    private static LegendHttpServer server;
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String MODEL = """
            ###Pure
            Class model::Person { firstName: String[1]; age: Integer[1]; }
            ###Relational
            Database store::DB ( Table T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), AGE INTEGER) )
            ###Mapping
            Mapping model::M ( model::Person: Relational { ~mainTable [store::DB] T_PERSON
                firstName: [store::DB] T_PERSON.FIRST_NAME, age: [store::DB] T_PERSON.AGE } )
            ###Connection
            RelationalDatabaseConnection store::Conn
            { store: store::DB; type: DuckDB; specification: DuckDB { path: '/nonexistent/never-opened.duckdb'; }; auth: Test; }
            ###Runtime
            Runtime test::RT { mappings: [ model::M ]; connections: [ store::DB: [ c: store::Conn ] ]; }
            """;

    @BeforeAll
    static void start() throws Exception {
        // the shipped server: lite's own with legend-engine's API mounted (D1.0b, 2026-11-12)
        server = com.legend.edge.EdgeServer.loopback(0);
        server.start();
    }

    @AfterAll
    static void stop() {
        server.stop();
    }

    private static HttpResponse<String> post(String path, String body) throws Exception {
        return HTTP.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + server.getPort() + "/api/pure/v1" + path))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** E1 then E9, as a client does: the query's lambda JSON, then its plan. */
    private static HttpResponse<String> plan(String query) throws Exception {
        HttpResponse<String> lambda = post("/grammar/grammarToJson/lambda", query);
        assertEquals(200, lambda.statusCode(), lambda.body());
        String input = "{\"clientVersion\":\"vX_X_X\",\"function\":" + lambda.body()
                + ",\"model\":" + Json.toCompact(Map.of("_type", "text", "code", MODEL))
                + ",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"test::RT\"}"
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        return post("/execution/generatePlan", input);
    }

    @Test
    @DisplayName("a relation query plans to SQL, with no database opened")
    void relationQueryPlans() throws Exception {
        HttpResponse<String> r = plan("|#>{store::DB.T_PERSON}#->filter(x|$x.AGE > 30)->select(~[FIRST_NAME])");
        assertEquals(200, r.statusCode(), r.body());
        String sql = sqlOf(r.body());
        assertTrue(sql.contains("T_PERSON") && sql.contains("30"), sql);
    }

    @Test
    @DisplayName("a class query plans to SQL through its mapping, with no database opened")
    void classQueryPlans() throws Exception {
        HttpResponse<String> r = plan("|model::Person.all()->filter(p|$p.age > 30)->project(~[n: p|$p.firstName])");
        assertEquals(200, r.statusCode(), r.body());
        String sql = sqlOf(r.body());
        assertTrue(sql.contains("T_PERSON") && sql.contains("30"), sql);
    }

    @Test
    @DisplayName("a query that does not compile answers the engine's error shape (generatePlan: 500)")
    void compileError() throws Exception {
        HttpResponse<String> r = plan("|model::Person.all()->project(~[n: p|$p.noSuchProperty])");
        assertEquals(500, r.statusCode(), r.body());
        Json.Obj o = Json.parseObject(r.body());
        assertEquals("error", o.getString("status"));
        assertTrue(o.getString("message").contains("noSuchProperty"), r.body());
    }

    @Test
    @DisplayName("a runtime written out in the request is refused by name, never dropped for the query's own (S301)")
    void aRuntimeWrittenOutIsRefusedByName() throws Exception {
        HttpResponse<String> lambda = post("/grammar/grammarToJson/lambda",
                "|#>{store::DB.T_PERSON}#->select(~[FIRST_NAME])->from(test::RT)");
        assertEquals(200, lambda.statusCode(), lambda.body());
        String input = "{\"clientVersion\":\"vX_X_X\",\"function\":" + lambda.body()
                + ",\"model\":" + Json.toCompact(Map.of("_type", "text", "code", MODEL))
                + ",\"runtime\":{\"_type\":\"engineRuntime\",\"mappings\":[],\"connections\":[],\"connectionStores\":[]}"
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        HttpResponse<String> r = post("/execution/generatePlan", input);
        // until 2026-11-12 the written-out runtime was read as none, and the query's ->from(test::RT) planned instead
        assertNotEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("a runtime written out in the request (engineRuntime)"), r.body());
    }

    @Test
    @DisplayName("only POST, and only legend-engine's own paths")
    void onlyPostAndOnlyUpstreamPaths() throws Exception {
        HttpResponse<String> get = HTTP.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + server.getPort()
                        + "/api/pure/v1/execution/generatePlan")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(405, get.statusCode());
        assertEquals(404, post("/execution/somethingOfOurOwn", "{}").statusCode());
    }

    @Test
    @DisplayName("the made-up /engine/plan and /engine/execute are gone")
    void theMadeUpEndpointsAreGone() throws Exception {
        for (String path : new String[] {"/engine/plan", "/engine/execute"}) {
            HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + server.getPort() + path))
                    .POST(HttpRequest.BodyPublishers.ofString("{}")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(404, r.statusCode(), path);
        }
    }

    private static String sqlOf(String plan) {
        Json.Obj p = Json.parseObject(plan);
        Json.Obj node = (Json.Obj) p.getObj("rootExecutionNode").getArr("executionNodes").items().get(0);
        return node.getString("sqlQuery");
    }
}
