// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.json.Json;
import com.legend.protocol.ProtocolTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * legend-engine's two protocol GET routes ({@code PureProtocol.java} of the pinned release, 4.145.0):
 * {@code getClassifierPathMap} and {@code getSubtypeInfo}, which a client (legend-studio's graph manager) awaits when
 * it starts. Their content is the protocol's one table ({@link ProtocolTypes}); the dispatcher answers them on every
 * host ({@link PureV1Api#route}); the HTTP server serves each route by the engine's own verb and no other.
 */
class ProtocolRoutesTest {

    private static final String MAP = "/api/pure/v1/protocol/pure/getClassifierPathMap";
    private static final String SUBTYPES = "/api/pure/v1/protocol/pure/getSubtypeInfo";

    private static LegendHttpServer server;
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeAll
    static void start() throws Exception {
        server = new LegendHttpServer(0);
        server.start();
    }

    @AfterAll
    static void stop() {
        server.stop();
    }

    private static HttpResponse<String> send(String method, String path, String... headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + server.getPort() + path))
                .method(method, "GET".equals(method) || "DELETE".equals(method)
                        ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString("{}"));
        for (int i = 0; i < headers.length; i += 2) {
            b.header(headers[i], headers[i + 1]);
        }
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** The dispatcher's answer: what every host gives (the server, the tab, Python's library). */
    private static PureV1Api.Answer routed(String path) {
        return PureV1Api.route(path, null, "", (query, runtime, output) -> {
            throw new AssertionError("a protocol route runs nothing");
        });
    }

    private static List<String> strings(Json.Arr arr) {
        List<String> out = new ArrayList<>();
        for (Json.Node n : arr.items()) {
            out.add(((Json.Str) n).value());
        }
        return out;
    }

    @Test
    @DisplayName("getClassifierPathMap: the engine's 37 rows, each a type and its classifier path, in the engine's form")
    void classifierPathMap() {
        PureV1Api.Answer a = routed(MAP);
        assertEquals(200, a.status());
        assertEquals("application/json", a.contentType());
        // the engine writes the array by hand, compact, type before classifierPath (PureProtocol.java:69)
        assertTrue(a.json().startsWith(
                "[{\"type\":\"association\",\"classifierPath\":\"meta::pure::metamodel::relationship::Association\"},"),
                a.json());
        List<Json.Node> rows = ((Json.Arr) Json.parse(a.json())).items();
        assertEquals(37, rows.size());
        Set<String> types = new LinkedHashSet<>();
        for (Json.Node n : rows) {
            Json.Obj row = (Json.Obj) n;
            assertEquals(List.of("type", "classifierPath"), List.copyOf(row.fields().keySet()), a.json());
            assertTrue(types.add(row.getString("type")), "listed twice: " + row.getString("type"));
            assertTrue(row.getString("classifierPath").startsWith("meta::"), a.json());
            // the route and the SDLC's entities read one table
            assertEquals(row.getString("classifierPath"), ProtocolTypes.classifierPath(row.getString("type")));
        }
        assertTrue(types.containsAll(List.of("class", "Enumeration", "function", "relational", "service",
                "snowflakeApp", "MongoDatabase")), types.toString());
        // four core elements the engine's route leaves out and its serializer maps: an entity's path, no row
        for (String core : List.of("mapping", "connection", "runtime", "dataElement")) {
            assertTrue(!types.contains(core), core);
            assertNotNull(ProtocolTypes.classifierPath(core), core);
        }
        assertNull(ProtocolTypes.classifierPath("noSuchElementType"));
    }

    @Test
    @DisplayName("getSubtypeInfo: the engine's store and function activator subtypes (4.145.0), not legend-studio's fallback")
    void subtypeInfo() {
        PureV1Api.Answer a = routed(SUBTYPES);
        assertEquals(200, a.status());
        assertEquals("application/json", a.contentType());
        // read from the pinned sources (ProtocolTypes says where): the registered subtypes whose protocol class is a
        // Store, a FunctionActivator. `binding` is no store there, and five activators are not in Studio's fallback
        assertEquals("{\"storeSubtypes\":[\"deephavenStore\",\"elasticsearch7Store\",\"MongoDatabase\",\"relational\","
                + "\"serviceStore\"],\"functionActivatorSubtypes\":[\"bigQueryFunction\",\"DeephavenApp\",\"functionJar\","
                + "\"hostedService\",\"memSqlFunction\",\"snowflakeApp\",\"snowflakeM2MUdf\"]}", a.json());
        // each subtype is an element type the classifier map knows: one table, so the two routes cannot disagree
        Json.Obj o = Json.parseObject(a.json());
        for (String field : List.of("storeSubtypes", "functionActivatorSubtypes")) {
            for (String type : strings(o.getArr(field))) {
                assertNotNull(ProtocolTypes.classifierPath(type), type);
            }
        }
    }

    @Test
    @DisplayName("over HTTP: GET answers both routes with the dispatcher's bytes, to a loopback page too")
    void servedByGet() throws Exception {
        for (String path : List.of(MAP, SUBTYPES)) {
            HttpResponse<String> plain = send("GET", path);
            assertEquals(200, plain.statusCode(), plain.body());
            assertEquals(routed(path).json(), plain.body());
            assertEquals("application/json", plain.headers().firstValue("Content-Type").orElse(""));

            // a browser-like client (jsdom, as legend-studio's tests run) names its page: a loopback origin is let
            // through, the CORS answer naming it
            HttpResponse<String> fromPage = send("GET", path, "Origin", "http://localhost");
            assertEquals(200, fromPage.statusCode(), fromPage.body());
            assertEquals(routed(path).json(), fromPage.body());
            assertEquals("http://localhost",
                    fromPage.headers().firstValue("Access-Control-Allow-Origin").orElse(""));

            // and a page elsewhere is refused before the route is asked, as on every route
            assertEquals(403, send("GET", path, "Origin", "https://attacker.example").statusCode());
        }
    }

    @Test
    @DisplayName("over HTTP: each route answers the engine's own verb alone -- any other is 405")
    void theWrongVerbIsStillRefused() throws Exception {
        // the two GET routes take no POST (the engine declares them @GET)
        for (String path : List.of(MAP, SUBTYPES)) {
            for (String verb : List.of("POST", "PUT", "DELETE")) {
                assertEquals(405, send(verb, path).statusCode(), verb + " " + path);
            }
        }
        // and every other pure/v1 path still takes POST alone: a route the API serves, one beside the two GET
        // routes, one it does not serve, and lite's own
        for (String path : List.of("/api/pure/v1/execution/generatePlan", "/api/pure/v1/grammar/grammarToJson/model",
                "/api/pure/v1/protocol/pure/getSomethingElse", "/api/pure/v1/protocol/pure/getSubtypeInfo/more",
                "/api/pure/v1/protocol/pure", "/api/pure/v1/noSuchRoute", "/api/lite/v1/compilation/compile")) {
            for (String verb : List.of("GET", "PUT", "DELETE")) {
                HttpResponse<String> r = send(verb, path);
                assertEquals(405, r.statusCode(), verb + " " + path + ": " + r.body());
            }
        }
        // a POST on a path that is no route is the API's 404, as before
        assertEquals(404, send("POST", "/api/pure/v1/protocol/pure/getSomethingElse").statusCode());
    }

    @Test
    @DisplayName("the verb list is the two routes and nothing else")
    void theGetRoutes() {
        assertEquals(Set.of(MAP, SUBTYPES), PureV1Api.GET_ROUTES);
    }
}
