// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * ONE REQUEST NEVER FREEZES THE SERVER (C5-J's batch, 4(a)-(b), 2026-11-16). Until then {@code LegendHttpServer.start}
 * gave the JDK server no executor, so every request ran on its one dispatcher thread: a request that did not finish --
 * a client that sent its headers and never its body, a query that ran long -- held every route. Each request now runs on
 * its own virtual thread, so what requests share is guarded: the LSP's open documents (a message is handled whole, one
 * at a time), and each database (one request at a time behind its session's lock: UsersH2DatabaseRequestsTest). And a
 * request whose handling throws an Error (an OutOfMemoryError, an assertion) is answered 500, where the exchange was
 * dropped unanswered.
 */
class ServerConcurrencyTest {

    private static HttpResponse<String> post(HttpClient client, int port, String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                        .timeout(Duration.ofSeconds(20)).POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void aStalledRequestHoldsNoOtherRoute() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0);
        server.start();
        try (Socket stalled = new Socket("127.0.0.1", server.getPort())) {
            // a request whose body never arrives: its handler waits on the read for as long as the client keeps it open
            OutputStream out = stalled.getOutputStream();
            out.write(("POST /lsp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n"
                    + "Content-Length: 100\r\n\r\n{").getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Thread.sleep(200);
            // another route answers meanwhile
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            HttpResponse<String> answered = post(client, server.getPort(),
                    "/api/pure/v1/grammar/grammarToJson/lambda", "|1");
            assertEquals(200, answered.statusCode(), answered.body());
        } finally {
            server.stop();
        }
    }

    /** Beyond the ceiling a request queues, never refused (P1, the user's ruling of 2026-11-16): with a ceiling of one,
     *  a stalled request holds the one permit, and another waits until it ends -- then is answered. The product's
     *  ceiling is {@link LegendHttpServer#MAX_CONCURRENT_REQUESTS}. */
    @Test
    void aRequestBeyondTheCeilingQueuesUntilOneEnds() throws Exception {
        assertEquals(1024, LegendHttpServer.MAX_CONCURRENT_REQUESTS);
        LegendHttpServer server = new LegendHttpServer(0, 1);
        server.start();
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            CompletableFuture<HttpResponse<String>> queued;
            try (Socket stalled = new Socket("127.0.0.1", server.getPort())) {
                OutputStream out = stalled.getOutputStream();
                out.write(("POST /lsp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n"
                        + "Content-Length: 100\r\n\r\n{").getBytes(StandardCharsets.US_ASCII));
                out.flush();
                Thread.sleep(200);
                queued = client.sendAsync(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getPort()
                                + "/api/pure/v1/grammar/grammarToJson/lambda"))
                        .timeout(Duration.ofSeconds(20)).POST(HttpRequest.BodyPublishers.ofString("|1")).build(),
                        HttpResponse.BodyHandlers.ofString());
                Thread.sleep(1000);
                assertFalse(queued.isDone(), "a request beyond the ceiling waits while the one permit is held");
            }
            // the stalled request ends with its socket: the queued one runs
            HttpResponse<String> answered = queued.get(20, TimeUnit.SECONDS);
            assertEquals(200, answered.statusCode(), answered.body());
        } finally {
            server.stop();
        }
    }

    /** An Error -- here an AssertionError: JUnit rethrows an OutOfMemoryError from any test as unrecoverable, so the
     *  probe is an Error of another kind, answered the same way. */
    @Test
    void anErrorIsAnswered() {
        PureV1Api.Answer answer = PureV1Api.answer(500, null, () -> {
            throw new AssertionError("probe");
        });
        assertEquals(500, answer.status());
        assertTrue(answer.json().contains("AssertionError: probe"), answer.json());
    }

    /** An Error a handler throws past its own catch -- the LSP's parse of a text nested deeper than its stack, a
     *  StackOverflowError its {@code catch (Exception)} does not take -- is answered 500 by the route, where the JDK
     *  server dropped the exchange and the client read no answer at all. */
    @Test
    void anErrorPastAHandlerIsAnswered() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0);
        server.start();
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            String text = "function test::f():Any[*]{" + "[".repeat(200000) + "1" + "]".repeat(200000) + "}";
            HttpResponse<String> r = post(client, server.getPort(), "/lsp", "{\"jsonrpc\":\"2.0\",\"method\":"
                    + "\"textDocument/didOpen\",\"params\":{\"textDocument\":{\"uri\":\"file:///deep.pure\","
                    + "\"languageId\":\"pure\",\"version\":1,\"text\":\"" + text + "\"}}}");
            assertEquals(500, r.statusCode(), r.body());
            assertTrue(r.body().contains("StackOverflowError"), r.body());
        } finally {
            server.stop();
        }
    }

    /** Many documents opened and changed at once: every message answered, none with the LSP's internal error, and the
     *  last diagnostics name every document. */
    @Test
    void concurrentLspMessagesAreHandledWhole() throws Exception {
        LegendHttpServer server = new LegendHttpServer(0);
        server.start();
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            int documents = 24;
            List<CompletableFuture<HttpResponse<String>>> sent = new ArrayList<>();
            for (int i = 0; i < documents; i++) {
                String uri = "file:///c5j-" + i + ".pure";
                for (String method : List.of("didOpen", "didChange")) {
                    String text = "Class test::C" + i + " { x: String[1]; }";
                    String params = method.equals("didOpen")
                            ? "{\"textDocument\":{\"uri\":\"" + uri + "\",\"languageId\":\"pure\",\"version\":1,"
                                    + "\"text\":\"" + text + "\"}}"
                            : "{\"textDocument\":{\"uri\":\"" + uri + "\",\"version\":2},"
                                    + "\"contentChanges\":[{\"text\":\"" + text + "\"}]}";
                    String body = "{\"jsonrpc\":\"2.0\",\"method\":\"textDocument/" + method + "\",\"params\":"
                            + params + "}";
                    sent.add(CompletableFuture.supplyAsync(() -> {
                        try {
                            return post(client, server.getPort(), "/lsp", body);
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }));
                }
            }
            for (CompletableFuture<HttpResponse<String>> f : sent) {
                HttpResponse<String> r = f.get(60, TimeUnit.SECONDS);
                assertEquals(200, r.statusCode(), r.body());
                assertFalse(r.body().contains("-32603"), r.body());
            }
            String last = post(client, server.getPort(), "/lsp", "{\"jsonrpc\":\"2.0\",\"method\":"
                    + "\"textDocument/didOpen\",\"params\":{\"textDocument\":{\"uri\":\"file:///c5j-last.pure\","
                    + "\"languageId\":\"pure\",\"version\":1,\"text\":\"Class test::Last { x: String[1]; }\"}}}").body();
            for (int i = 0; i < documents; i++) {
                assertTrue(last.contains("file:///c5j-" + i + ".pure"), "document " + i + " kept");
            }
        } finally {
            server.stop();
        }
    }
}
