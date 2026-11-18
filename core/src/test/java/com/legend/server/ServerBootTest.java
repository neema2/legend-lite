// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE SHIPPED SERVER'S BOOT (K2's server half, the user's ruling of 2026-11-18: "Product root, edge opt-in"): the
 * server starts at lite's own entry and mounts legend-engine's module only when its configuration names it -- with no
 * engine, lite's routes alone (no /api/pure/v1); with {@code legend-engine}, pure/v1 answers as legend-engine does; an
 * engine no module answers is refused by name. One artifact: the edge's module is on this test's class path, as it is
 * in the shipped jar, and only the configuration activates it.
 */
class ServerBootTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String LAMBDA = "/api/pure/v1/grammar/grammarToJson/lambda";

    private static HttpResponse<String> post(LegendHttpServer server, String path, String body) throws Exception {
        return HTTP.send(HttpRequest.newBuilder().uri(URI.create("http://localhost:" + server.getPort() + path))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static LegendHttpServer started(LegendHttpServer.Mount mount) throws Exception {
        LegendHttpServer server = new LegendHttpServer(0, java.net.InetAddress.getLoopbackAddress(),
                LegendHttpServer.Origins.LOOPBACK, null, LegendHttpServer.NO_SOURCES, mount);
        server.start();
        return server;
    }

    @Test
    void withNoEngineTheEdgeIsNotLoaded() throws Exception {
        assertSame(LegendHttpServer.LITE, LegendHttpServer.configured(null));
        LegendHttpServer server = started(LegendHttpServer.configured(null));
        try {
            assertEquals(200, post(server, "/api/lite/v1/compilation/compile",
                    "{\"_type\":\"text\",\"code\":\"Class a::A { x: Integer[1]; }\"}").statusCode());
            assertEquals(404, post(server, LAMBDA, "|1").statusCode());
        } finally {
            server.stop();
        }
    }

    @Test
    void withLegendEngineConfiguredPureV1Answers() throws Exception {
        LegendHttpServer server = started(LegendHttpServer.configured("legend-engine"));
        try {
            HttpResponse<String> lambda = post(server, LAMBDA, "|1");
            assertEquals(200, lambda.statusCode(), lambda.body());
            assertTrue(lambda.body().contains("\"_type\":\"lambda\""), lambda.body());
        } finally {
            server.stop();
        }
    }

    @Test
    void anEngineNoModuleAnswersIsRefusedByName() {
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> LegendHttpServer.configured("nope"));
        assertTrue(refused.getMessage().contains("'nope'") && refused.getMessage().contains("legend-engine"),
                refused.getMessage());
    }

    @Test
    void theEngineIsReadFromTheCommandLineThenTheEnvironment() {
        assertEquals("legend-engine", LegendHttpServer.engineOf(new String[] {"0", "--engine", "legend-engine"}, "x"));
        assertEquals("legend-engine", LegendHttpServer.engineOf(new String[] {"0"}, " legend-engine "));
        assertEquals(null, LegendHttpServer.engineOf(new String[] {"0"}, null));
        assertThrows(IllegalArgumentException.class, () -> LegendHttpServer.engineOf(new String[] {"--engine"}, null));
    }
}
