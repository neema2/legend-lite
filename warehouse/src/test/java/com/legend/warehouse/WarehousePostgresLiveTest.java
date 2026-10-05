package com.legend.warehouse;

import static com.legend.warehouse.WarehouseServerTest.API;
import static com.legend.warehouse.WarehouseServerTest.rowsOn;
import static com.legend.warehouse.WarehouseServerTest.runOn;
import static com.legend.warehouse.WarehouseServerTest.sendTo;
import static com.legend.warehouse.WarehouseServerTest.str;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.warehouse.server.Statements;
import com.legend.warehouse.sqlapi.SqlApi;
import com.legend.warehouse.sqlapi.SqlApi.Column;
import com.legend.warehouse.sqlapi.SqlApi.ErrorCode;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApiBinding;
import com.legend.warehouse.sqlapi.SqlApiBinding.HttpResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A Postgres catalog against a LIVE Postgres (docs/POSTGRES_DIALECT_HOMEWORK_2026_10_01.md, leg P0): selected only by
 * its own manual target (//warehouse:postgres_live; :tests leaves it out by selection, Bazel workplan P3-21), and
 * failing, not skipping, unless {@code LEGENDLITE_PG_DSN} names one (a login role with SELECT, nothing more), with
 * {@code LEGENDLITE_PG_EXTENSIONS} the directory holding {@code postgres_scanner.duckdb_extension}. Run by
 * {@code bazel test //warehouse:postgres_live --test_env=LEGENDLITE_PG_DSN=... --test_env=LEGENDLITE_PG_EXTENSIONS=...}
 * (tagged manual); with {@code WAREHOUSE_BINARY} set, against the native image. Needs no table: every query
 * makes its rows with generate_series. Embedded Postgres in the test environment is leg P2.
 */
class WarehousePostgresLiveTest {

    static TestServer server;
    static String alice;   // owner
    static String carol;   // reader, granted USAGE on pg
    static String dave;    // reader, granted nothing

    @BeforeAll
    static void start() throws Exception {
        String dsn = System.getenv("LEGENDLITE_PG_DSN");
        if (dsn == null || dsn.isEmpty()) throw new IllegalStateException("LEGENDLITE_PG_DSN: a live Postgres (--test_env)");
        String ext = System.getenv("LEGENDLITE_PG_EXTENSIONS");
        if (ext == null) throw new IllegalStateException("LEGENDLITE_PG_EXTENSIONS: the directory of DuckDB's postgres extension");
        server = TestServer.start(Files.createTempDirectory("warehouse-pg"),
                List.of(new String[] {"alice", "alice-pw"}, new String[] {"carol", "carol-pw"},
                        new String[] {"dave", "dave-pw"}),
                List.of("alice"), new Statements.Limits(4, 50, 1_000_000, Duration.ofMinutes(5)), List.of(),
                Map.of("pg", dsn), Path.of(ext).toAbsolutePath());
        alice = login("alice", "alice-pw");
        carol = login("carol", "carol-pw");
        dave = login("dave", "dave-pw");
        done(runOn(server, alice, StatementRequest.of("GRANT USAGE ON CATALOG pg TO carol")));
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) server.close();
    }

    static final String TYPED = """
            SELECT i::int8 AS id, i::int4 AS n, (i * 1.5)::numeric(18,6) AS amount, i % 2 = 0 AS even,
                   DATE '2024-01-01' + i AS d, TIMESTAMP '2024-01-01' + i * INTERVAL '1 second' AS ts,
                   TIMESTAMPTZ '2024-01-01 00:00:00+00' + i * INTERVAL '1 minute' AS tstz, i / 3.0::float8 AS dbl,
                   ARRAY['a' || i, 'b'] AS tags, 'row ' || i AS label, CASE WHEN i % 7 = 0 THEN NULL ELSE i END AS maybe,
                   9007199254740993::int8 AS big
            FROM generate_series(1, 25000) AS g(i)
            ORDER BY i DESC""";

    @Test
    void arrowCarriesTheSameValuesAsJson() throws Exception {
        WarehouseArrowTest.check(server, alice, "pg", TYPED, 10_000);
    }

    @Test
    void jsonRowsKeepPostgresOrderAndExactValues() throws Exception {
        SqlApiBinding.Done d = done(runOn(server, alice, new StatementRequest(TYPED, "pg", 60_000, 30_000, 30_000)));
        List<List<Json.Node>> rows = rowsOn(server, alice, d);
        assertEquals(25_000, rows.size());
        assertEquals("25000", str(rows.get(0).get(0)), "BIGINT as a string, in the inner ORDER BY's order");
        assertEquals("1", str(rows.get(24_999).get(0)));
        assertEquals("9007199254740993", str(rows.get(0).get(11)), "no loss past 2^53");
        List<String> names = d.status().result().columns().stream().map(Column::name).toList();
        assertEquals(List.of("id", "n", "amount", "even", "d", "ts", "tstz", "dbl", "tags", "label", "maybe", "big"), names);
    }

    @Test
    void describeOnlyAsksPostgresForTheColumns() throws Exception {
        SqlApiBinding.Done d = done(runOn(server, carol,
                new StatementRequest("SELECT 1::int4 AS a, 'x'::text AS b, now() AS c", "pg", 60_000, 30_000, 100).describe()));
        assertEquals(List.of("a", "b", "c"), d.status().result().columns().stream().map(Column::name).toList());
        assertEquals(0, d.status().result().rowCount());
    }

    @Test
    void postgresErrorsAndRefusalsReachTheClient() throws Exception {
        assertFailed(carol, "SELECT nope FROM generate_series(1, 2) AS g(i)", ErrorCode.SQL_BIND, "column \"nope\" does not exist");
        assertFailed(carol, "SELECT 1 AS a;", ErrorCode.BAD_REQUEST, "trailing ';'");
        assertFailed(carol, "SELECT 1; SELECT 2", ErrorCode.SQL_BIND, "cannot insert multiple commands");
        // breaking out of DuckDB's COPY (SELECT ... FROM (<sql>) ...) into a second statement: one prepared statement
        assertFailed(alice, "SELECT 1 AS a) AS q) TO STDOUT; CREATE TABLE wh_pwned (a int); --", ErrorCode.SQL_BIND, "syntax error");
        // DuckDB SQL is not Postgres SQL
        assertFailed(alice, "SELECT * FROM duckdb_tables()", ErrorCode.SQL_BIND, "duckdb_tables");
        // grants are managed from a DuckDB catalog
        assertFailed(alice, "GRANT USAGE ON CATALOG pg TO dave", ErrorCode.BAD_REQUEST, "managed from a DuckDB catalog");
        assertFailed(alice, "CREATE TABLE wh_t (a int)", ErrorCode.SQL_BIND, "must be a SELECT");
        // a line comment at the end does not swallow DuckDB's closing parenthesis
        assertEquals("1", str(rowsOn(server, carol, done(runOn(server, carol,
                new StatementRequest("SELECT 1::int8 AS a -- the end", "pg", 60_000, 30_000, 100)))).get(0).get(0)));
    }

    /**
     * The catalog's Postgres session is UTC whatever the server's configuration (leg B,
     * docs/DATACUBE_APP_PLAN_2026_10_02.md): a timestamptz's year, a comparison with a timestamp literal and
     * its text are its UTC instant's. The DSN under test sets no zone; the attach adds it.
     */
    @Test
    void aZonedTimestampReadsAsItsUtcInstant() throws Exception {
        List<List<Json.Node>> rows = rowsOn(server, carol, done(runOn(server, carol, new StatementRequest(
                "SELECT current_setting('TimeZone') AS z, (SELECT source FROM pg_settings WHERE name = 'TimeZone') AS src,"
                        + " extract(year FROM TIMESTAMPTZ '2024-12-31 23:30:00-05')::int8 AS y,"
                        + " (TIMESTAMPTZ '2024-12-31 23:30:00-05' >= TIMESTAMP '2025-01-01 04:30:00')::text AS at_utc",
                "pg", 60_000, 30_000, 100))));
        assertEquals(List.of("UTC", "client", "2025", "true"), rows.get(0).stream().map(WarehouseServerTest::str).toList());
    }

    @Test
    void aReaderNeedsUsageOfTheCatalog() throws Exception {
        assertFailed(dave, "SELECT 1 AS a", ErrorCode.FORBIDDEN, "no USAGE granted on catalog pg");
        // USAGE is for Postgres catalogs; main's objects are granted one by one
        assertFailedOn("main", alice, "GRANT USAGE ON CATALOG main TO dave", ErrorCode.BAD_REQUEST, "no attached catalog main");
        assertFailedOn("main", alice, "GRANT USAGE ON CATALOG pg TO nobody", ErrorCode.BAD_REQUEST, "no user or role nobody");
        // sessions are DuckDB's
        HttpResult session = sendTo(server, API.openSession("pg", carol));
        assertEquals(400, session.status(), session.body());
        // the objects are the attached database's, without Postgres' own schemas; a reader needs USAGE
        HttpResult objects = sendTo(server, API.objects("pg", alice));
        assertEquals(200, objects.status(), objects.body());
        assertFalse(objects.body().contains("\"schema\":\"pg_catalog\""), objects.body());
        assertFalse(objects.body().contains("\"schema\":\"information_schema\""), objects.body());
        assertEquals(403, sendTo(server, API.objects("pg", dave)).status());
        // every catalog's at once: an attached one names its database type; one without USAGE is left out
        List<SqlApi.CatalogObject> all = API.objects(sendTo(server, API.allObjects(alice)));
        assertTrue(all.stream().filter(o -> o.catalog().equals("pg")).allMatch(o -> o.databaseType().equals("Postgres")),
                all.toString());
        assertTrue(all.stream().filter(o -> o.catalog().equals("main")).allMatch(o -> o.databaseType().equals("DuckDB")));
        assertTrue(API.objects(sendTo(server, API.allObjects(dave))).stream().noneMatch(o -> o.catalog().equals("pg")));
    }

    @Test
    void aCancelStopsTheQueryInPostgres() throws Exception {
        String sql = "SELECT pg_sleep(20) AS slept, 'cancel-me' AS marker";
        HttpResult submitted = sendTo(server, API.submit(new StatementRequest(sql, "pg", 120_000, 0, 100), carol));
        String id = ((SqlApiBinding.Poll) API.next(submitted, carol)).statementId();
        long t0 = System.nanoTime();
        awaitBackends("cancel-me", 1);
        HttpResult cancelled = sendTo(server, API.cancel(id, carol));
        assertTrue(cancelled.body().contains("\"state\":\"cancelled\""), cancelled.body());
        awaitBackends("cancel-me", 0);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms < 10_000, "Postgres stopped the query " + ms + " ms after it started, not 20 s");
    }

    @Test
    void aTimeoutStopsTheQueryInPostgres() throws Exception {
        String sql = "SELECT pg_sleep(20) AS slept, 'time-me-out' AS marker";
        long t0 = System.nanoTime();
        SqlApiBinding.Step step = runOn(server, carol, new StatementRequest(sql, "pg", 1_500, 30_000, 100));
        SqlApiBinding.Failed f = assertInstanceOf(SqlApiBinding.Failed.class, step);
        assertEquals(ErrorCode.TIMEOUT, f.error().code(), f.error().message());
        awaitBackends("time-me-out", 0);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms < 10_000, "timed out and stopped in Postgres after " + ms + " ms");
    }

    /**
     * Waits (up to 15 s) until Postgres runs exactly {@code n} backends whose query contains {@code marker},
     * asked through the catalog itself: the login role sees its own backends' text.
     */
    static void awaitBackends(String marker, int n) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        long seen = -1;
        while (System.nanoTime() < deadline) {
            String q = "SELECT count(*)::int8 AS n FROM pg_stat_activity WHERE state = 'active'"
                    + " AND strpos(query, '" + marker + "') > 0 AND pid <> pg_backend_pid()";
            List<List<Json.Node>> rows = rowsOn(server, alice, done(runOn(server, alice,
                    new StatementRequest(q, "pg", 60_000, 30_000, 100))));
            seen = Long.parseLong(str(rows.get(0).get(0)));
            if (seen == n) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Postgres runs " + seen + " backends with '" + marker + "', not " + n);
    }

    static void assertFailed(String token, String sql, ErrorCode code, String fragment) throws Exception {
        assertFailedOn("pg", token, sql, code, fragment);
    }

    static void assertFailedOn(String catalog, String token, String sql, ErrorCode code, String fragment) throws Exception {
        SqlApiBinding.Step step = runOn(server, token, new StatementRequest(sql, catalog, 60_000, 30_000, 100));
        SqlApiBinding.Failed f = assertInstanceOf(SqlApiBinding.Failed.class, step, sql);
        assertEquals(code, f.error().code(), sql + ": " + f.error().message());
        assertTrue(f.error().message().contains(fragment), sql + ": " + f.error().message());
    }

    static SqlApiBinding.Done done(SqlApiBinding.Step step) {
        if (step instanceof SqlApiBinding.Failed f) throw new AssertionError(f.error().code() + ": " + f.error().message());
        return (SqlApiBinding.Done) step;
    }

    static String login(String user, String password) throws Exception {
        return API.token(sendTo(server, API.login(user, password))).token();
    }
}
