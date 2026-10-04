package com.legend.warehouse.server;

import com.legend.testing.Runfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The warehouse as the single-user DataCube app (docs/DATACUBE_APP_PLAN_2026_10_02.md, A1): Postgres URLs
 * as catalogs, the launch key, the site and its {@code /config.json}. Postgres itself is not needed: the
 * live attach is WarehousePostgresLiveTest's.
 */
class AppModeTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    // -- Postgres URLs ---------------------------------------------------------------------------

    @Test
    void aPostgresUrlIsACatalogNamedAfterItsDatabase() {
        PostgresUrl u = PostgresUrl.parse("postgresql://bob@db.internal:5433/shop");
        assertEquals("shop", u.catalog());
        assertEquals("host='db.internal' port='5433' dbname='shop' user='bob' options='-c statement_timeout=60000'",
                u.dsn());
        // no host: libpq's own default (the local socket); parameters as given
        assertEquals("dbname='shop' sslmode='require' options='-c statement_timeout=60000'",
                PostgresUrl.parse("postgres:///shop?sslmode=require").dsn());
        // a percent-encoded user and password, decoded and quoted
        assertEquals("host='db' dbname='shop' user='bo b' password='p@ss:w\\'d' options='-c statement_timeout=60000'",
                PostgresUrl.parse("postgresql://bo%20b:p%40ss%3Aw'd@db/shop").dsn());
        // the URL's own options replace the default timeout
        assertEquals("host='db' dbname='shop' options='-c statement_timeout=5000'",
                PostgresUrl.parse("postgresql://db/shop?options=-c%20statement_timeout%3D5000").dsn());
        assertEquals("host='::1' dbname='shop' options='-c statement_timeout=60000'",
                PostgresUrl.parse("postgresql://[::1]/shop").dsn());
    }

    @Test
    void aPostgresUrlThatCannotBeACatalogIsRefusedByName() {
        for (String bad : List.of(
                "postgresql://db",                  // no database
                "postgresql://db/",
                "postgresql://db/Shop",             // not a catalog name: --postgres NAME=DSN
                "postgresql://db/shop/extra",
                "postgresql://h1,h2/shop",          // several hosts
                "postgresql://db/shop?sslmode",     // not key=value
                "postgresql://db/shop?dbname=x")) { // given twice
            assertThrows(IllegalArgumentException.class, () -> PostgresUrl.parse(bad), bad);
        }
    }

    @Test
    void anAskedPasswordIsQuotedIntoTheConnectionString() {
        assertEquals("host='db' dbname='shop' password='it\\'s \\\\ fine'",
                PostgresUrl.withPassword("host='db' dbname='shop'", "it's \\ fine".toCharArray()));
    }

    // -- the command line ------------------------------------------------------------------------

    @Test
    void theAppsCommandLine() throws Exception {
        WarehouseServer.CommandLine c = WarehouseServer.commandLine(new String[] {
            "postgresql://bob@db/shop", "--site", "/srv/cube", "--single-user", "--open", "--table", "sales.orders",
            "--duckdb-extensions", "/opt/ext"});
        assertEquals(Map.of("shop", "host='db' dbname='shop' user='bob' options='-c statement_timeout=60000'"),
                c.config().postgres());
        assertEquals(Path.of("/srv/cube"), c.config().site());
        assertEquals(System.getProperty("user.name"), c.config().singleUser());
        assertTrue(c.open());
        assertEquals("sales.orders", c.table());
        // single-user without --data: a fresh directory, removed on exit
        assertNotNull(c.temporaryData());
        assertEquals(c.temporaryData(), c.config().dataDir());
        assertNull(WarehouseServer.commandLine(new String[] {"--single-user", "--data", "/var/cube"}).temporaryData());
        // a plain warehouse is what it was
        WarehouseServer.CommandLine plain = WarehouseServer.commandLine(new String[] {"--user", "alice:pw"});
        assertNull(plain.config().site());
        assertNull(plain.config().singleUser());
        assertNull(plain.table());
        assertNull(plain.temporaryData());
    }

    @Test
    void theAppsCommandLineRefusesWhatCannotWork() {
        for (String[] args : List.of(
                new String[] {"--single-user", "--user", "alice:pw"},             // one user: the launch key's
                new String[] {"--single-user", "--owner", "alice"},
                new String[] {"--open", "--site", "/srv/cube"},                     // --open needs --single-user
                new String[] {"--open", "--single-user"},                           // and --site
                new String[] {"--single-user", "--table", "sales.orders"},          // --table needs a page
                new String[] {"--site", "/s", "--single-user", "--open", "--table", "orders"},     // schema.name
                new String[] {"postgresql://db/shop", "postgresql://other/shop", "--duckdb-extensions", "/x"},
                new String[] {"postgresql://db/shop", "--postgres", "shop=host=db", "--duckdb-extensions", "/x"})) {
            assertThrows(IllegalArgumentException.class, () -> WarehouseServer.commandLine(args), String.join(" ", args));
        }
    }

    @Test
    void underBazelRunARelativeDataDirectoryIsWhereTheCommandWasStarted() throws Exception {
        Path startedIn = Path.of("work").toAbsolutePath();
        // the default, and a relative --data: where `bazel run` was started, wherever the launcher put the server
        assertEquals(startedIn.resolve("warehouse-data"),
                WarehouseServer.commandLine(new String[] {}, startedIn).config().dataDir());
        assertEquals(startedIn.resolve("cube"),
                WarehouseServer.commandLine(new String[] {"--data", "cube"}, startedIn).config().dataDir());
        // an absolute --data is what it says
        Path absolute = Path.of("srv", "cube").toAbsolutePath();
        assertEquals(absolute,
                WarehouseServer.commandLine(new String[] {"--data", absolute.toString()}, startedIn).config().dataDir());
        // not under `bazel run`: as given, relative to wherever the server runs
        assertEquals(Path.of("warehouse-data"), WarehouseServer.commandLine(new String[] {}, null).config().dataDir());
    }

    @Test
    void theSingleUserIsTheAccountRunningItWithWhatAPrincipalCannotHoldWrittenAsUnderscores() {
        assertEquals("neema", Identity.accountPrincipal("neema"));
        // Windows account names may hold spaces, and a domain account reads DOMAIN\name
        assertEquals("John_Madsen", Identity.accountPrincipal("John Madsen"));
        assertEquals("CORP_jo", Identity.accountPrincipal("CORP\\jo"));
        assertEquals("Jos_", Identity.accountPrincipal("José"));
        // one underscore per character, a character outside the BMP included
        assertEquals("a_b", Identity.accountPrincipal("a😀b"));
        // nothing to keep: refused, by the name
        assertThrows(IllegalArgumentException.class, () -> Identity.accountPrincipal(""));
        assertThrows(IllegalArgumentException.class, () -> Identity.accountPrincipal("x".repeat(129)));
    }

    // -- the launch key --------------------------------------------------------------------------

    @Test
    void theLaunchKeySignsItsUserInWhileTheServerRuns() {
        Identity identity = new Identity(new byte[32], Duration.ofMinutes(5), Clock.systemUTC());
        String key = identity.launchKey("neema");
        Identity.Issued first = identity.loginWithKey(key);
        assertNotNull(first);
        assertEquals("neema", first.principal());
        assertEquals("neema", identity.verify(first.token()));
        // a reload signs in again with the same key
        assertNotNull(identity.loginWithKey(key));
        assertNull(identity.loginWithKey(key.substring(1)));
        assertNull(identity.loginWithKey("not base64 !"));
        assertThrows(IllegalStateException.class, () -> identity.launchKey("neema"));
        // a server without one signs nobody in by key
        assertNull(new Identity(new byte[32], Duration.ofMinutes(5), Clock.systemUTC()).loginWithKey(key));
    }

    // -- the site --------------------------------------------------------------------------------

    @Test
    void theSiteIsServedBesideTheApiToLoopbackOnly(@TempDir Path dir) throws Exception {
        Path site = Files.createDirectories(dir.resolve("site"));
        Files.writeString(site.resolve("index.html"), "<html>cube</html>");
        Files.createDirectories(site.resolve("vendor"));
        Files.write(site.resolve("vendor/planner.wasm"), new byte[] {0, 'a', 's', 'm'});
        Files.writeString(dir.resolve("secret.txt"), "outside the site");
        try (WarehouseServer s = singleUser(dir, site)) {
            String base = "http://127.0.0.1:" + s.port();
            HttpResponse<String> index = get(base + "/");
            assertEquals(200, index.statusCode());
            assertEquals("<html>cube</html>", index.body());
            assertEquals("text/html; charset=utf-8", index.headers().firstValue("Content-Type").orElseThrow());
            HttpResponse<String> wasm = get(base + "/vendor/planner.wasm");
            assertEquals("application/wasm", wasm.headers().firstValue("Content-Type").orElseThrow());
            assertEquals(404, get(base + "/missing.js").statusCode());
            // a path that climbs out of the site, written so no client normalizes it away first
            assertTrue(raw(s.port(), "GET /%2e%2e/secret.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                    .startsWith("HTTP/1.1 404"));
            // the page's configuration names the origin the browser used: one origin, no CORS
            assertEquals("{\"warehouse\":\"http://127.0.0.1:" + s.port() + "\"}", get(base + "/config.json").body());
            assertTrue(raw(s.port(), "GET /config.json HTTP/1.1\r\nHost: localhost:" + s.port()
                    + "\r\nConnection: close\r\n\r\n").endsWith("{\"warehouse\":\"http://localhost:" + s.port() + "\"}"));
            // a name that is not this machine's (DNS rebinding) is refused the page
            assertTrue(raw(s.port(), "GET / HTTP/1.1\r\nHost: cube.attacker.example\r\nConnection: close\r\n\r\n")
                    .startsWith("HTTP/1.1 403"));
            // the API is unchanged beside it
            assertEquals(401, get(base + "/sql/v1/catalogs").statusCode());
        }
    }

    @Test
    void theSingleUserSignsInWithTheLaunchKeyOnly(@TempDir Path dir) throws Exception {
        try (WarehouseServer s = singleUser(dir, null)) {
            String base = "http://127.0.0.1:" + s.port();
            String key = s.launchKey();
            assertNotNull(key);
            HttpResponse<String> signedIn = post(base + "/sql/v1/login", "{\"key\":\"" + key + "\"}");
            assertEquals(200, signedIn.statusCode(), signedIn.body());
            Json.Obj token = Json.parseObject(signedIn.body());
            assertEquals(System.getProperty("user.name"), token.getString("principal"));
            HttpResponse<String> catalogs = HTTP.send(HttpRequest.newBuilder(URI.create(base + "/sql/v1/catalogs"))
                    .header("Authorization", "Bearer " + token.getString("token")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, catalogs.statusCode(), catalogs.body());
            assertEquals(401, post(base + "/sql/v1/login", "{\"key\":\"" + key.substring(2) + "\"}").statusCode());
            assertEquals(401, post(base + "/sql/v1/login", "{\"user\":\"" + System.getProperty("user.name")
                    + "\",\"password\":\"\"}").statusCode());
        }
    }

    private static WarehouseServer singleUser(Path dir, Path site) throws Exception {
        return new WarehouseServer(new WarehouseServer.Config(0, dir.resolve("data"), List.of("main"), List.of(), null,
                Duration.ofMinutes(5), new Statements.Limits(2, 10, 1000, Duration.ofMinutes(1))).withSite(site)
                .withSingleUser(System.getProperty("user.name"))
                .withDuckdbLibrary(Runfile.property("warehouse.duckdb.library")));
    }

    private static HttpResponse<String> get(String url) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(String url, String body) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    /** A request written byte for byte: the Host header and the path as given, unnormalized. */
    private static String raw(int port, String request) throws IOException {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            OutputStream out = socket.getOutputStream();
            out.write(request.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            InputStream in = socket.getInputStream();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
