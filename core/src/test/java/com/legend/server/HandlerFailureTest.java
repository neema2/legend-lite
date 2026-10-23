// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a handler lets through reaches its client as a 500, at once, and the server goes on answering. An independent
 * review (2026-10-23): with the requests on a thread of their own ({@code REQUEST_STACK_BYTES}), a handler's Error (an
 * AssertionError in the probe; a StackOverflowError or OutOfMemoryError the handlers' own catches let through) left the
 * client waiting for its timeout, where the JDK's dispatcher had closed the connection.
 */
class HandlerFailureTest {

    @Test
    void aHandlersErrorIsA500AndTheServerGoesOn() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0);
        server.route("/test/error", exchange -> {
            throw new AssertionError("a handler's error");
        });
        server.route("/test/exception", exchange -> {
            throw new IllegalStateException("a handler's exception");
        });
        server.start();
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            for (String path : new String[] {"/test/error", "/test/exception", "/test/error"}) {
                long t0 = System.nanoTime();
                HttpResponse<String> response = client.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + server.getPort() + path)).timeout(Duration.ofSeconds(5))
                        .GET().build(), HttpResponse.BodyHandlers.ofString());
                long ms = (System.nanoTime() - t0) / 1_000_000;
                assertEquals(500, response.statusCode(), path + ": " + response.body());
                assertTrue(response.body().contains("internal error"), response.body());
                assertTrue(ms < 4_000, path + " answered in " + ms + " ms");
            }
        } finally {
            server.stop();
        }
    }
}
