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
 * THE SERVER'S TWO ASSEMBLIES (phase D's D1.0b, the orchestrator's ruling of 2026-11-12): lite's own server answers
 * lite's routes and none of legend-engine's (they are the edge's, G5); the shipped server ({@code edge.EdgeServer})
 * answers both. The saved-query store is the product's, reached by lite's own route and, on the shipped server, by
 * legend-engine's path too: one store, both routes.
 */
class EdgeServerTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String LAMBDA = "/api/pure/v1/grammar/grammarToJson/lambda";

    private static final String COMPILE_EVERY_ERROR = "/api/lite/v1/compilation/compile";

    private static final String MODEL = "{\"_type\":\"text\",\"code\":\"Class a::A { x: Integer[1]; }\"}";

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

    @Test
    void liteAloneAnswersLitesRoutesAndNoneOfUpstreams() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0);
        server.start();
        try {
            assertEquals(200, send(server, "POST", COMPILE_EVERY_ERROR, MODEL).statusCode());
            assertEquals(404, send(server, "POST", LAMBDA, "|1").statusCode());
            assertEquals(404, send(server, "GET", "/api/pure/v1/query/q1", null).statusCode());
            assertEquals(404, send(server, "GET", "/api/server/v1/currentUser", null).statusCode());
        } finally {
            server.stop();
        }
    }

    @Test
    void theShippedServerAnswersBoth() throws Exception {
        LegendHttpServer server = com.legend.edge.EdgeServer.loopback(0);
        server.start();
        try {
            assertEquals(200, send(server, "POST", COMPILE_EVERY_ERROR, MODEL).statusCode());
            HttpResponse<String> lambda = send(server, "POST", LAMBDA, "|1");
            assertEquals(200, lambda.statusCode(), lambda.body());
            assertTrue(lambda.body().contains("\"_type\":\"lambda\""), lambda.body());
            assertEquals(200, send(server, "GET", "/api/server/v1/currentUser", null).statusCode());
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
            HttpResponse<String> made = send(server, "POST", "/api/lite/v1/query", query);
            assertEquals(200, made.statusCode(), made.body());
            HttpResponse<String> upstream = send(server, "GET", "/api/pure/v1/query/q1", null);
            assertEquals(200, upstream.statusCode(), upstream.body());
            assertTrue(upstream.body().contains("\"name\":\"one\""), upstream.body());
            HttpResponse<String> lite = send(server, "GET", "/api/lite/v1/query/q1", null);
            assertEquals(200, lite.statusCode(), lite.body());
            // the same record (an open stamps its view time, so the two bodies differ there alone)
            assertEquals(createdAt(upstream.body()), createdAt(lite.body()));
            assertTrue(lite.body().contains("\"name\":\"one\""), lite.body());
        } finally {
            server.stop();
        }
    }
}
