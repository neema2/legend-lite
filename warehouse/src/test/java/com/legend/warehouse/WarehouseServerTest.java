package com.legend.warehouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.warehouse.client.WarehouseClient;
import com.legend.warehouse.server.Statements;
import com.legend.warehouse.sqlapi.NativeBinding;
import com.legend.warehouse.sqlapi.SqlApi;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.Chunk;
import com.legend.warehouse.sqlapi.SqlApi.ErrorCode;
import com.legend.warehouse.sqlapi.SqlApi.State;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApi.Status;
import com.legend.warehouse.sqlapi.SqlApiBinding;
import com.legend.warehouse.sqlapi.SqlApiBinding.HttpCall;
import com.legend.warehouse.sqlapi.SqlApiBinding.HttpResult;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The warehouse over its own HTTP API, driven through the binding exactly
 * as a client drives it: a real server on a free port, a temporary data
 * directory, two users.
 */
class WarehouseServerTest {

    static TestServer server;
    static Path data;
    static final HttpClient HTTP = HttpClient.newHttpClient();
    static final SqlApiBinding API = new NativeBinding(2_000);

    @BeforeAll
    static void start() throws Exception {
        data = Files.createTempDirectory("warehouse-test");
        server = TestServer.start(data, List.of(new String[] {"alice", "alice-pw"}, new String[] {"bob", "bob-pw"}),
                new Statements.Limits(2, 50, 1_000_000, Duration.ofMinutes(5)));
    }

    @AfterAll
    static void stop() throws Exception {
        server.close();
    }

    // -- a minimal driver: the binding decides, this performs ---------------

    static HttpResult send(HttpCall c) throws Exception {
        return sendTo(server, c);
    }

    static HttpResult sendTo(TestServer server, HttpCall c) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + c.path()));
        c.headers().forEach(b::header);
        String body = c.body();
        b.method(c.method(), body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body));
        HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
        return new HttpResult(r.statusCode(), r.body());
    }

    static String login(String user, String password) throws Exception {
        return API.token(send(API.login(user, password))).token();
    }

    /** Run to the end: every step the binding asks for. */
    static SqlApiBinding.Step run(String token, StatementRequest req) throws Exception {
        return runOn(server, token, req);
    }

    static SqlApiBinding.Step runOn(TestServer server, String token, StatementRequest req) throws Exception {
        SqlApiBinding.Step step = API.next(sendTo(server, API.submit(req, token)), token);
        while (step instanceof SqlApiBinding.Poll p) step = API.next(sendTo(server, p.call()), token);
        return step;
    }

    /** Every row of a finished statement, across its chunks. */
    static List<List<Json.Node>> rows(String token, SqlApiBinding.Done done) throws Exception {
        return rowsOn(server, token, done);
    }

    static List<List<Json.Node>> rowsOn(TestServer server, String token, SqlApiBinding.Done done) throws Exception {
        Status s = done.status();
        List<List<Json.Node>> out = new ArrayList<>();
        int chunks = s.result().chunkCount();
        if (chunks == 0) return out;   // a statement with no result rows: DDL, a write
        Chunk first = s.firstChunk();
        assertNotNull(first);
        out.addAll(first.rows());
        for (int i = 1; i < chunks; i++) out.addAll(API.chunk(sendTo(server, API.fetchChunk(s.statementId(), i, token))).rows());
        return out;
    }

    static List<List<Json.Node>> query(String token, String sql) throws Exception {
        SqlApiBinding.Step step = run(token, StatementRequest.of(sql));
        if (step instanceof SqlApiBinding.Failed f) throw new AssertionError(f.error().code() + ": " + f.error().message());
        return rows(token, (SqlApiBinding.Done) step);
    }

    static String str(Json.Node n) {
        return ((Json.Str) n).value();
    }

    // -- the tests ------------------------------------------------------------

    @Test
    void loginRefusesAWrongPasswordAndAnUnknownUser() throws Exception {
        assertEquals(401, send(API.login("alice", "nope")).status());
        assertEquals(401, send(API.login("mallory", "alice-pw")).status());
    }

    @Test
    void everyCallNeedsAValidToken() throws Exception {
        HttpResult noToken = send(new HttpCall("POST", "/sql/v1/statements",
                java.util.Map.of("Content-Type", "application/json"), "{\"sql\":\"SELECT 1\"}"));
        assertEquals(401, noToken.status());
        String good = login("alice", "alice-pw");
        String forged = good.substring(0, good.indexOf('.')) + ".AAAA";
        HttpResult bad = send(API.submit(StatementRequest.of("SELECT 1"), forged));
        assertEquals(401, bad.status());
        assertTrue(bad.body().contains("AUTH_INVALID"), bad.body());
    }

    @Test
    void aStatementRunsAndItsValuesFollowTheTypeRules() throws Exception {
        String t = login("alice", "alice-pw");
        List<List<Json.Node>> r = query(t, """
                SELECT 42::INTEGER AS i, 9007199254740993::BIGINT AS big, 1.5::DOUBLE AS d,
                       12.345::DECIMAL(10,3) AS dec, 'x' AS s, DATE '2024-02-29' AS dt,
                       TIMESTAMP '2024-02-29 13:14:15' AS ts, true AS b, NULL::INTEGER AS n""");
        List<Json.Node> row = r.get(0);
        assertEquals(42, ((Json.Num) row.get(0)).longValue());
        assertEquals("9007199254740993", str(row.get(1)), "BIGINT travels as a string: no precision lost");
        assertEquals(1.5, ((Json.Num) row.get(2)).doubleValue());
        assertEquals("12.345", str(row.get(3)));
        assertEquals("x", str(row.get(4)));
        assertEquals("2024-02-29", str(row.get(5)));
        assertEquals("2024-02-29T13:14:15", str(row.get(6)));
        assertEquals(true, ((Json.Bool) row.get(7)).value());
        assertTrue(row.get(8) instanceof Json.Null);
    }

    @Test
    void eachStatementCarriesItsOwnUsersIdentityEvenAtTheSameTime() throws Exception {
        String a = login("alice", "alice-pw");
        String b = login("bob", "bob-pw");
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<String[]>> seen = new ArrayList<>();
            for (int i = 0; i < 24; i++) {
                String who = i % 2 == 0 ? a : b;
                String expect = i % 2 == 0 ? "alice" : "bob";
                seen.add(pool.submit((Callable<String[]>) () -> new String[] {expect,
                        str(query(who, "SELECT system.main.authenticated_user()").get(0).get(0))}));
            }
            for (Future<String[]> f : seen) {
                String[] pair = f.get();
                assertEquals(pair[0], pair[1], "a statement saw another user's identity");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void noStatementCanChangeWhoTheUserIs() throws Exception {
        String b = login("bob", "bob-pw");
        String session = API.session(send(API.openSession("main", b))).sessionId();
        // the old way to claim an identity, and a temp macro over the identity function's name
        for (String attempt : List.of("SET VARIABLE app_user = 'alice'", "CREATE TEMP MACRO authenticated_user() AS 'alice'")) {
            assertTrue(run(b, StatementRequest.of(attempt).inSession(session)) instanceof SqlApiBinding.Done, attempt);
        }
        SqlApiBinding.Done done = (SqlApiBinding.Done) run(b, StatementRequest.of(
                "SELECT system.main.authenticated_user(), current_user, session_user, user").inSession(session));
        assertEquals(List.of("bob", "bob", "bob", "bob"), done.status().firstChunk().rows().get(0).stream().map(n -> str(n)).toList());
        send(API.closeSession(session, b));
    }

    @Test
    void anAclViewShowsEachUserTheirOwnRows() throws Exception {
        String a = login("alice", "alice-pw");
        String b = login("bob", "bob-pw");
        query(a, """
                CREATE OR REPLACE TABLE trades AS SELECT * FROM (VALUES (1, 'EMEA'), (2, 'APAC'), (3, 'AMER')) t(id, region);
                CREATE OR REPLACE TABLE acl AS SELECT * FROM (VALUES ('alice', 'EMEA'), ('bob', 'APAC'), ('bob', 'AMER')) a(username, region);
                CREATE OR REPLACE VIEW my_trades AS SELECT * FROM trades t WHERE EXISTS (
                  SELECT 1 FROM acl a WHERE a.username = system.main.authenticated_user() AND a.region = t.region);
                SELECT 1""");
        assertEquals(List.of("EMEA"), query(a, "SELECT region FROM my_trades ORDER BY 1").stream().map(r -> str(r.get(0))).toList());
        assertEquals(List.of("AMER", "APAC"), query(b, "SELECT region FROM my_trades ORDER BY 1").stream().map(r -> str(r.get(0))).toList());
    }

    @Test
    void anotherUsersStatementDoesNotExistToYou() throws Exception {
        String a = login("alice", "alice-pw");
        String b = login("bob", "bob-pw");
        SqlApiBinding.Done done = (SqlApiBinding.Done) run(a, StatementRequest.of("SELECT 1"));
        String id = done.status().statementId();
        assertEquals(404, send(API.fetchChunk(id, 0, b)).status());
        assertEquals(200, send(API.fetchChunk(id, 0, a)).status());
    }

    @Test
    void anotherUsersSessionDoesNotExistToYou() throws Exception {
        String a = login("alice", "alice-pw");
        String b = login("bob", "bob-pw");
        String session = API.session(send(API.openSession("main", a))).sessionId();
        HttpResult asBob = send(API.submit(StatementRequest.of("SELECT 1").inSession(session), b));
        assertEquals(404, asBob.status(), asBob.body());
        HttpResult asAlice = send(API.submit(StatementRequest.of("SELECT system.main.authenticated_user()").inSession(session), a));
        assertEquals(200, asAlice.status(), asAlice.body());
        assertTrue(asAlice.body().contains("alice"), asAlice.body());
        assertEquals(404, send(API.closeSession(session, b)).status());
        assertEquals(200, send(API.closeSession(session, a)).status());
        assertEquals(404, send(API.submit(StatementRequest.of("SELECT 1").inSession(session), a)).status(),
                "a closed session is gone");
    }

    @Test
    void aLargeResultComesInChunks() throws Exception {
        String t = login("alice", "alice-pw");
        SqlApiBinding.Done done = (SqlApiBinding.Done) run(t, new StatementRequest(
                "SELECT i FROM range(25000) r(i) ORDER BY i", "main", 60_000, 5_000, 10_000));
        assertEquals(25_000, done.status().result().rowCount());
        assertEquals(3, done.status().result().chunkCount());
        List<List<Json.Node>> all = rows(t, done);
        assertEquals(25_000, all.size());
        assertEquals("24999", str(all.get(24_999).get(0)));
    }

    @Test
    void errorsCarryTheirKind() throws Exception {
        String t = login("alice", "alice-pw");
        ApiError parse = ((SqlApiBinding.Failed) run(t, StatementRequest.of("SELEC 1"))).error();
        assertEquals(ErrorCode.SQL_PARSE, parse.code());
        ApiError bind = ((SqlApiBinding.Failed) run(t, StatementRequest.of("SELECT * FROM no_such_table"))).error();
        assertEquals(ErrorCode.SQL_BIND, bind.code());
        ApiError catalog = ((SqlApiBinding.Failed) run(t,
                new StatementRequest("SELECT 1", "nope", 1_000, 1_000, 10))).error();
        assertEquals(ErrorCode.NOT_FOUND, catalog.code());
    }

    @Test
    void aSlowStatementTimesOutAndOneCanBeCancelled() throws Exception {
        String t = login("alice", "alice-pw");
        String slow = "SELECT count(*) FROM range(10000000000) a(i) WHERE i % 7 = 3";
        ApiError e = ((SqlApiBinding.Failed) run(t, new StatementRequest(slow, "main", 300, 10_000, 100))).error();
        assertEquals(ErrorCode.TIMEOUT, e.code(), e.message());

        HttpResult started = send(API.submit(new StatementRequest(slow, "main", 60_000, 200, 100), t));
        assertEquals(202, started.status(), started.body());
        String id = ((SqlApiBinding.Poll) API.next(started, t)).statementId();
        HttpResult cancelled = send(API.cancel(id, t));
        assertTrue(cancelled.body().contains("\"cancelled\""), cancelled.body());
    }

    /** Result bytes the server holds: {in memory, spilled to files} ({@code GET /health}). */
    static long[] held(TestServer on) throws Exception {
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + on.port() + "/health"))
                .build(), HttpResponse.BodyHandlers.ofString());
        Json.Obj results = (Json.Obj) Json.parseObject(r.body()).get("results");
        return new long[] {results.getLong("inMemoryBytes"), results.getLong("spilledBytes")};
    }

    @Test
    void aResultTheClientIsDoneWithIsFreedAtOnce() throws Exception {
        String t = login("alice", "alice-pw");
        long before = held(server)[0];
        SqlApiBinding.Done done = (SqlApiBinding.Done) run(t, new StatementRequest(
                "SELECT i, 'r' || i AS s FROM range(30000) r(i)", "main", 60_000, 5_000, 10_000));
        String id = done.status().statementId();
        assertTrue(held(server)[0] > before, "a finished result is held until fetched");
        assertEquals(200, send(API.closeStatement(id, t)).status());
        assertEquals(before, held(server)[0], "closing it gives the memory back at once");
        assertEquals(404, send(API.fetchChunk(id, 1, t)).status(), "and its chunks are gone");
    }

    @Test
    void theClientFreesWhatItHasRead() throws Exception {
        WarehouseClient client = new WarehouseClient(URI.create("http://127.0.0.1:" + server.port() + "/"), API);
        client.login("alice", "alice-pw");
        long before = held(server)[0];
        for (SqlApi.ResultFormat f : SqlApi.ResultFormat.values()) {
            WarehouseClient.Result r = client.execute(new StatementRequest(
                    "SELECT i FROM range(30000) r(i)", "main", 60_000, 5_000, 10_000).as(f));
            assertEquals(30_000, r.rows().size());
        }
        assertEquals(before, held(server)[0], "every chunk read, the client closes the statement");
    }

    @Test
    void aResultPastTheMemoryBudgetSpillsToFilesAndReadsTheSame() throws Exception {
        Path dir = Files.createTempDirectory("warehouse-spill");
        try (TestServer small = TestServer.start(dir, List.<String[]>of(new String[] {"alice", "alice-pw"}),
                new Statements.Limits(2, 10, 1_000_000, Duration.ofMinutes(5), 1L << 20))) {
            TestServer saved = server;
            server = small;
            try {
                String t = login("alice", "alice-pw");
                SqlApiBinding.Done done = (SqlApiBinding.Done) run(t, new StatementRequest(
                        "SELECT i, 'row ' || i AS s FROM range(200000) r(i) ORDER BY i", "main", 60_000, 30_000, 20_000));
                long[] h = held(small);
                assertTrue(h[0] <= 1L << 20, "memory stays within the budget: " + h[0]);
                assertTrue(h[1] > 0, "the rest went to files");
                List<List<Json.Node>> rows = rows(t, done);
                assertEquals(200_000, rows.size());
                for (int i : new int[] {0, 19_999, 20_000, 123_456, 199_999}) {
                    assertEquals(Integer.toString(i), str(rows.get(i).get(0)));
                    assertEquals("row " + i, str(rows.get(i).get(1)));
                }
                assertEquals(200, send(API.closeStatement(done.status().statementId(), t)).status());
                assertEquals(0, held(small)[1], "closing it deletes the files");
            } finally {
                server = saved;
            }
        }
    }

    @Test
    void aFullQueueRefusesInsteadOfGrowing() throws Exception {
        Path dir = Files.createTempDirectory("warehouse-queue");
        try (TestServer small = TestServer.start(dir, List.<String[]>of(new String[] {"alice", "alice-pw"}), new Statements.Limits(1, 1, 1_000, Duration.ofMinutes(5)))) {
            TestServer saved = server;
            server = small;
            try {
                String t = login("alice", "alice-pw");
                String slow = "SELECT count(*) FROM range(10000000000) a(i) WHERE i % 7 = 3";
                StatementRequest noWait = new StatementRequest(slow, "main", 10_000, 0, 100);
                HttpResult running = send(API.submit(noWait, t));
                HttpResult queued = send(API.submit(noWait, t));
                HttpResult refused = send(API.submit(noWait, t));
                assertEquals(202, running.status(), running.body());
                assertEquals(202, queued.status(), queued.body());
                assertEquals(503, refused.status(), refused.body());
                assertTrue(refused.body().contains("QUEUE_FULL"), refused.body());
                send(API.cancel(((SqlApiBinding.Poll) API.next(running, t)).statementId(), t));
                send(API.cancel(((SqlApiBinding.Poll) API.next(queued, t)).statementId(), t));
            } finally {
                server = saved;
            }
        }
    }

    @Test
    void writesWorkForTheirUserAndEveryStatementIsInTheHistory() throws Exception {
        String t = login("bob", "bob-pw");
        query(t, "CREATE OR REPLACE TABLE bob_notes AS SELECT 1 AS id, 'hello' AS note");
        List<List<Json.Node>> r = query(t, "SELECT note FROM bob_notes");
        assertEquals("hello", str(r.get(0).get(0)));
        // the history, through the API: the caller's own statements, newest first
        List<SqlApi.HistoryEntry> h = API.history(send(API.history(2, t)));
        assertEquals(List.of("SELECT note FROM bob_notes", "CREATE OR REPLACE TABLE bob_notes AS SELECT 1 AS id, 'hello' AS note"),
                h.stream().map(SqlApi.HistoryEntry::sql).toList());
        assertEquals(State.SUCCEEDED, h.get(0).state());
        assertEquals(1L, h.get(0).rowCount());
        String a = login("alice", "alice-pw");
        assertTrue(API.history(send(API.history(1000, a))).stream().noneMatch(e -> e.sql().contains("bob_notes")),
                "another user's statements are not in your history");
    }

    @Test
    void aStatementRefusedForAnUnsupportedTypeSaysWhich() throws Exception {
        String t = login("alice", "alice-pw");
        // Lists, structs and maps are carried (WarehouseJdbcTest); a UNION is not yet.
        ApiError e = ((SqlApiBinding.Failed) run(t, StatementRequest.of(
                "SELECT union_value(k := 1) AS u"))).error();
        assertEquals(ErrorCode.UNSUPPORTED_TYPE, e.code());
        assertTrue(e.message().contains("'u'") && e.message().contains("UNION"), e.message());
    }

    @Test
    void theStatusOfAFinishedStatementIsStable() throws Exception {
        String t = login("alice", "alice-pw");
        SqlApiBinding.Done done = (SqlApiBinding.Done) run(t, StatementRequest.of("SELECT 7"));
        HttpResult again = send(new HttpCall("GET", "/sql/v1/statements/" + done.status().statementId(),
                java.util.Map.of("Authorization", "Bearer " + t), null));
        Status s = com.legend.warehouse.sqlapi.ApiJson.parseStatus(again.body());
        assertEquals(State.SUCCEEDED, s.state());
        assertNull(s.firstChunk(), "only the first answer carries the first chunk");
        assertEquals(1, s.result().rowCount());
    }
}
