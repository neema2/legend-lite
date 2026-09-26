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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code POST /engine/plan}: Pure to SQL WITHOUT executing — the one planner a
 * client-side executor (DataCube's browser plane) asks for its SQL. Through the
 * real HTTP server. The model's connection names a database file that does not
 * exist: planning must never open it.
 */
class PlanEndpointTest {

    private static LegendHttpServer server;
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String SOURCE = """
            ###Pure
            Class model::Person { firstName: String[1]; age: Integer[1]; }
            ###Relational
            Database store::DB ( Table T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), AGE INTEGER) )
            ###Mapping
            Mapping model::M ( model::Person: Relational { ~mainTable [store::DB] T_PERSON
                firstName: [store::DB] T_PERSON.FIRST_NAME, age: [store::DB] T_PERSON.AGE } )
            ###Connection
            RelationalDatabaseConnection store::Conn
            { type: DuckDB; specification: DuckDB { path: '/nonexistent/never-opened.duckdb'; }; auth: Test; }
            ###Runtime
            Runtime test::RT { mappings: [ model::M ]; connections: [ store::DB: [ c: store::Conn ] ]; }
            """;

    @BeforeAll
    static void start() throws Exception {
        server = new LegendHttpServer(0);
        server.start();
    }

    @AfterAll
    static void stop() {
        server.stop();
    }

    private static com.legend.json.Json.Obj post(String query) throws Exception {
        String body = "{\"code\":\"" + Json.escape(SOURCE + "\n" + query) + "\",\"runtime\":\"test::RT\"}";
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + server.getPort() + "/engine/plan"))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode(), r.body());
        return Json.parseObject(r.body());
    }

    @Test
    @DisplayName("a class query plans to SQL, with no database opened")
    void plans() throws Exception {
        var r = post("model::Person.all()->filter(p|$p.age > 30)->project(~[n: p|$p.firstName])");
        assertEquals(new Json.Bool(true), r.get("success"), r.toString());
        String sql = ((Json.Str) r.get("sql")).value();
        assertTrue(sql.contains("SELECT") && sql.contains("T_PERSON") && sql.contains("30"), sql);
    }

    @Test
    @DisplayName("a query that does not compile answers an error, not an internal failure")
    void compileError() throws Exception {
        var r = post("model::Person.all()->project(~[n: p|$p.noSuchProperty])");
        assertEquals(new Json.Bool(false), r.get("success"), r.toString());
        assertTrue(((Json.Str) r.get("error")).value().contains("noSuchProperty"), r.toString());
        assertFalse(r.fields().containsKey("internal"), "an honest compile error is not internal: " + r);
    }

    @Test
    @DisplayName("only POST")
    void onlyPost() throws Exception {
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + server.getPort() + "/engine/plan")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(405, r.statusCode());
    }
}
