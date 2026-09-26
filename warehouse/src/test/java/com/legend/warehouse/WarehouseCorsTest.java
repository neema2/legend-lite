package com.legend.warehouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.warehouse.server.Statements;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A web page on another origin may call the warehouse only when the warehouse names that origin
 * ({@code --allow-origin}): DataCube's Direct mode, where the page's own WebAssembly module speaks
 * the SQL API (docs/WAREHOUSE_D1_DESIGN_2026_09_26.md). The browser enforces CORS; these tests read
 * the headers it reads.
 */
class WarehouseCorsTest {

    static final String PAGE = "https://cube.example.com";
    static final String OTHER = "https://evil.example.com";
    static TestServer server;
    static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeAll
    static void start() throws Exception {
        server = TestServer.start(Files.createTempDirectory("warehouse-cors"),
                List.<String[]>of(new String[] {"alice", "alice-pw"}), List.of("alice"),
                new Statements.Limits(2, 50, 1_000_000, Duration.ofMinutes(5)), List.of(PAGE));
    }

    @AfterAll
    static void stop() throws Exception {
        server.close();
    }

    static HttpResponse<String> send(String method, String path, String origin, String body, String... headers)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + path))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (origin != null) b.header("Origin", origin);
        for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    static Optional<String> allowOrigin(HttpResponse<?> r) {
        return r.headers().firstValue("Access-Control-Allow-Origin");
    }

    static HttpResponse<String> preflight(String origin) throws Exception {
        return send("OPTIONS", "/sql/v1/statements", origin, null,
                "Access-Control-Request-Method", "POST", "Access-Control-Request-Headers", "authorization,content-type");
    }

    @Test
    void anAllowedPagesPreflightIsAnswered() throws Exception {
        HttpResponse<String> r = preflight(PAGE);
        assertEquals(204, r.statusCode());
        assertEquals(Optional.of(PAGE), allowOrigin(r));
        String methods = r.headers().firstValue("Access-Control-Allow-Methods").orElse("");
        String headers = r.headers().firstValue("Access-Control-Allow-Headers").orElse("");
        assertTrue(methods.contains("POST") && methods.contains("GET") && methods.contains("DELETE"), methods);
        assertTrue(headers.contains("Authorization") && headers.contains("Content-Type"), headers);
    }

    @Test
    void anyOtherPagesPreflightIsRefused() throws Exception {
        HttpResponse<String> r = preflight(OTHER);
        assertEquals(403, r.statusCode());
        assertEquals(Optional.empty(), allowOrigin(r));
    }

    @Test
    void theAllowedPageReadsAnswersAndErrorsAlike() throws Exception {
        HttpResponse<String> ok = send("POST", "/sql/v1/login", PAGE, "{\"user\":\"alice\",\"password\":\"alice-pw\"}",
                "Content-Type", "application/json");
        assertEquals(200, ok.statusCode());
        assertEquals(Optional.of(PAGE), allowOrigin(ok));
        // an error must reach the page too, or it cannot say why
        HttpResponse<String> denied = send("POST", "/sql/v1/login", PAGE, "{\"user\":\"alice\",\"password\":\"wrong\"}",
                "Content-Type", "application/json");
        assertEquals(401, denied.statusCode());
        assertEquals(Optional.of(PAGE), allowOrigin(denied));
        HttpResponse<String> unauthenticated = send("GET", "/sql/v1/catalogs", PAGE, null);
        assertEquals(401, unauthenticated.statusCode());
        assertEquals(Optional.of(PAGE), allowOrigin(unauthenticated));
    }

    @Test
    void noOtherOriginAndNoOriginGetsTheHeader() throws Exception {
        assertEquals(Optional.empty(), allowOrigin(send("GET", "/health", OTHER, null)));
        assertEquals(Optional.empty(), allowOrigin(send("GET", "/health", null, null)));
    }
}
