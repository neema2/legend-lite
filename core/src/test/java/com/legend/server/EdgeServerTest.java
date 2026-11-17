// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE SERVER'S TWO ASSEMBLIES (phase D's D1.0b, the orchestrator's ruling of 2026-11-12; lite's API v1 spec §6, the
 * user's go of 2026-11-17: one server, the edge a module of its own): lite's own server -- booted with the edge module
 * OFF -- answers every one of lite's operations and none of legend-engine's routes (they are the edge's, G5); the
 * shipped server ({@code edge.EdgeServer}) answers both. The saved-query store is the product's, reached by lite's own
 * route ({@code /api/lite/v1/queries}) and, on the shipped server, by legend-engine's path too: one store, both routes.
 */
class EdgeServerTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String LAMBDA = "/api/pure/v1/grammar/grammarToJson/lambda";

    private static final String MODEL_CHECK = "/api/lite/v1/model/check";

    private static final String MODEL = "{\"model\":\"Class a::A { x: Integer[1]; }\"}";

    /** Every one of lite's operations (spec §2), with a request each answers (or refuses as the model's or the
     *  query's, never as a path it does not serve). */
    private static final java.util.Map<String, String> LITE_OPERATIONS = liteOperations();

    private static java.util.Map<String, String> liteOperations() {
        String query = com.legend.door.Parse.lambdaJson("|1", false);
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        out.put("model/check", MODEL);
        out.put("model/elements", MODEL);
        out.put("query/parse", "{\"text\":\"|1\"}");
        out.put("query/print", "{\"query\":" + query + "}");
        out.put("query/type", "{\"model\":\"\",\"query\":" + query + "}");
        out.put("query/plan", "{\"model\":\"\",\"query\":" + query + ",\"runtime\":\"a::RT\"}");
        out.put("query/run", "{\"model\":\"\",\"query\":" + query + ",\"runtime\":\"a::RT\",\"format\":\"json\"}");
        out.put("model/createClass", "{}");
        out.put("model/createAssociation", "{}");
        out.put("model/createDataSpace", "{}");
        out.put("ts/item", "{\"text\":\"a = b\"}");
        out.put("ts/statement", "{\"model\":\"\",\"query\":" + query + "}");
        out.put("user", "");
        return java.util.Collections.unmodifiableMap(out);
    }

    /** legend-engine's routes: the edge's, served by its module alone. */
    private static final java.util.List<String> ENGINE_ROUTES = java.util.List.of(LAMBDA,
            "/api/pure/v1/compilation/compile", "/api/pure/v1/compilation/lambdaRelationType",
            "/api/pure/v1/execution/execute", "/api/pure/v1/execution/generatePlan", "/api/pure/v1/query",
            "/api/server/v1/currentUser");

    private static HttpResponse<String> send(LegendHttpServer server, String method, String path, String body)
            throws Exception {
        return HTTP.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + server.getPort() + path))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static String createdAt(String record) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"createdAt\":(\\d+)").matcher(record);
        assertTrue(m.find(), record);
        return m.group(1);
    }

    /** THE BOOT TEST WITH THE EDGE MODULE OFF (spec §6): every one of lite's operations is served -- answered, or
     *  refused as the model's or the query's, never as a path lite does not serve -- and every one of legend-engine's
     *  routes is absent, 404. The store answers at lite's own route, in lite's shape; the user is the store's. */
    @Test
    void liteAloneAnswersLitesRoutesAndNoneOfUpstreams() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0);
        server.start();
        try {
            for (java.util.Map.Entry<String, String> op : LITE_OPERATIONS.entrySet()) {
                HttpResponse<String> r = send(server, "POST", "/api/lite/v1/" + op.getKey(), op.getValue());
                assertTrue(r.statusCode() != 404 && !r.body().contains("no such legend-lite API"),
                        op.getKey() + ": " + r.statusCode() + " " + r.body());
                assertTrue(r.body().isEmpty() || !r.body().contains("\"errorType\""), op.getKey() + ": " + r.body());
            }
            assertEquals(200, send(server, "POST", MODEL_CHECK, MODEL).statusCode());
            for (String route : ENGINE_ROUTES) {
                assertEquals(404, send(server, "POST", route, "{}").statusCode(), route);
                assertEquals(404, send(server, "GET", route, null).statusCode(), route);
            }
            assertEquals(404, send(server, "GET", "/api/pure/v1/query/q1", null).statusCode());
            // who saved queries belong to: the store's own user, lite's user operation (the edge program's H7, the
            // orchestrator's Q-b ruling of 2026-11-17; spec §2)
            HttpResponse<String> user = send(server, "POST", "/api/lite/v1/user", null);
            assertEquals(200, user.statusCode(), user.body());
            assertEquals("{\"user\":\"" + com.legend.queries.SavedQueries.ANONYMOUS + "\"}", user.body());
            // a server with no store refuses the store's calls in lite's shape: internal, its status the store's
            HttpResponse<String> noStore = send(server, "GET", "/api/lite/v1/queries/q1", null);
            assertEquals(500, noStore.statusCode(), noStore.body());
            assertEquals("internal", com.legend.json.Json.parseObject(noStore.body()).getString("kind"), noStore.body());
        } finally {
            server.stop();
        }
    }

    @Test
    void theShippedServerAnswersBoth() throws Exception {
        LegendHttpServer server = com.legend.edge.EdgeServer.loopback(0);
        server.start();
        try {
            assertEquals(200, send(server, "POST", MODEL_CHECK, MODEL).statusCode());
            HttpResponse<String> lambda = send(server, "POST", LAMBDA, "|1");
            assertEquals(200, lambda.statusCode(), lambda.body());
            assertTrue(lambda.body().contains("\"_type\":\"lambda\""), lambda.body());
            assertEquals(200, send(server, "GET", "/api/server/v1/currentUser", null).statusCode());
            // pure/v1 reads the decoded path, as its handler always did, and answers an unknown path in its old words
            // (the D part 1 audit's nits, 2026-11-15)
            HttpResponse<String> encoded = send(server, "POST", "/api/pure/v1/grammar/grammarToJson%2Flambda", "|1");
            assertEquals(200, encoded.statusCode(), encoded.body());
            HttpResponse<String> unknown = send(server, "POST", "/api/pure/v1/no/such/route", "{}");
            assertEquals(404, unknown.statusCode(), unknown.body());
            assertTrue(unknown.body().contains("no such legend-engine API in legend-lite"), unknown.body());
        } finally {
            server.stop();
        }
    }

    @Test
    void bothRoutesReachTheOneStore(@TempDir Path store) throws Exception {
        LegendHttpServer server = new LegendHttpServer(0, java.net.InetAddress.getLoopbackAddress(),
                LegendHttpServer.Origins.LOOPBACK, store, LegendHttpServer.NO_SOURCES,
                com.legend.edge.EdgeServer.UPSTREAM);
        server.start();
        try {
            String query = "{\"id\":\"q1\",\"name\":\"one\",\"groupId\":\"demo\",\"artifactId\":\"trading\","
                    + "\"versionId\":\"0.0.0\",\"content\":\"|demo::trading::Trade.all()\","
                    + "\"executionContext\":{\"_type\":\"explicitExecutionContext\","
                    + "\"mapping\":\"demo::trading::TradingMapping\",\"runtime\":\"demo::trading::H2Runtime\"},"
                    + "\"taggedValues\":[]}";
            HttpResponse<String> made = send(server, "POST", "/api/lite/v1/queries", query);
            assertEquals(200, made.statusCode(), made.body());
            HttpResponse<String> upstream = send(server, "GET", "/api/pure/v1/query/q1", null);
            assertEquals(200, upstream.statusCode(), upstream.body());
            assertTrue(upstream.body().contains("\"name\":\"one\""), upstream.body());
            HttpResponse<String> lite = send(server, "GET", "/api/lite/v1/queries/q1", null);
            assertEquals(200, lite.statusCode(), lite.body());
            // the same record (an open stamps its view time, so the two bodies differ there alone)
            assertEquals(createdAt(upstream.body()), createdAt(lite.body()));
            assertTrue(lite.body().contains("\"name\":\"one\""), lite.body());
            // the store's user is lite's user operation: legend-engine's store path reads `currentUser` as a query's
            // id, as upstream's does, and has none by it
            HttpResponse<String> user = send(server, "POST", "/api/lite/v1/user", null);
            assertEquals("{\"user\":\"" + com.legend.queries.SavedQueries.ANONYMOUS + "\"}", user.body());
            HttpResponse<String> edge = send(server, "GET", "/api/pure/v1/query/currentUser", null);
            assertEquals(404, edge.statusCode(), edge.body());
            // a saved query not found: lite's shape at lite's route, kind request, the store's 404; legend-engine's
            // {"message"} at the edge's
            HttpResponse<String> missing = send(server, "GET", "/api/lite/v1/queries/nope", null);
            assertEquals(404, missing.statusCode(), missing.body());
            assertEquals("{\"kind\":\"request\",\"message\":\"no saved query has the ID 'nope'\"}", missing.body());
            HttpResponse<String> missingEdge = send(server, "GET", "/api/pure/v1/query/nope", null);
            assertEquals(404, missingEdge.statusCode(), missingEdge.body());
            assertTrue(missingEdge.body().startsWith("{\"message\":"), missingEdge.body());
        } finally {
            server.stop();
        }
    }
}
