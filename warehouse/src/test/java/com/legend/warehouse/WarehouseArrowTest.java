package com.legend.warehouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.warehouse.server.Statements;
import com.legend.warehouse.sqlapi.NativeBinding;
import com.legend.warehouse.sqlapi.SqlApi.ResultFormat;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApi.Status;
import com.legend.warehouse.sqlapi.SqlApiBinding;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The warehouse's Arrow chunks (W1d): the same statement as JSON and as Arrow, every Arrow chunk read
 * by pyarrow (a standard Arrow reader, test-only) and every value compared with the JSON API's
 * (src/test/python/arrow_matches_json.py). Where pyarrow is missing the test is skipped, loudly; CI's
 * app lane installs it and sets WAREHOUSE_ARROW_CHECK=required, which turns a skip into a failure.
 */
class WarehouseArrowTest {

    static TestServer server;
    static String token;
    static final HttpClient HTTP = HttpClient.newHttpClient();
    static final SqlApiBinding API = new NativeBinding(2_000);

    @BeforeAll
    static void start() throws Exception {
        server = TestServer.start(Files.createTempDirectory("warehouse-arrow"),
                List.<String[]>of(new String[] {"alice", "alice-pw"}), new Statements.Limits(2, 50, 1_000_000, Duration.ofMinutes(5)));
        token = API.token(WarehouseServerTest.sendTo(server, API.login("alice", "alice-pw"))).token();
        WarehouseServerTest.runOn(server, token, StatementRequest.of("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"));
    }

    @AfterAll
    static void stop() throws Exception {
        server.close();
    }

    @Test
    void everyTypeCarriesTheSameValuesAsItsJson() throws Exception {
        check("""
                SELECT * FROM (VALUES
                  (true, 1::TINYINT, 2::SMALLINT, 3::INTEGER, 9007199254740993::BIGINT,
                   170141183460469231731687303715884105727::HUGEINT, 255::UTINYINT, 65535::USMALLINT,
                   4294967295::UINTEGER, 18446744073709551615::UBIGINT, 340282366920938463463374607431768211455::UHUGEINT,
                   1.5::FLOAT, -0.0::DOUBLE, 123.45::DECIMAL(9,2), 1.5::DECIMAL(38,10), 'héllo', repeat('x', 40),
                   '00000000-0000-0000-0000-000000000042'::UUID, INTERVAL '1 year 2 hours 3.5 seconds', 'ok'::mood,
                   '{"a":[1,2]}'::JSON, '\\x41\\x00'::BLOB, DATE '0001-01-01', TIME '01:02:03.456789',
                   TIMESTAMP '1500-01-01 01:02:03.123456', TIMESTAMP_S '2024-01-02 03:04:05',
                   TIMESTAMP_MS '2024-01-02 03:04:05.12', TIMESTAMP_NS '2024-01-02 03:04:05.123456789',
                   TIMESTAMPTZ '2024-01-02 03:04:05+05', [1, NULL, 3], [1, 2]::INTEGER[2], {'x': 1, 'y': 'b'},
                   MAP {'k': [1]}, [{'e': 'sad'::mood, 't': TIMESTAMP '2024-01-01 00:00:00'}], 'nan'::DOUBLE),
                  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                   NULL, NULL, NULL),
                  (false, -1::TINYINT, -2::SMALLINT, -3::INTEGER, (-9223372036854775807 - 1)::BIGINT,
                   (-170141183460469231731687303715884105727 - 1)::HUGEINT, 0::UTINYINT, 0::USMALLINT, 0::UINTEGER,
                   0::UBIGINT, 0::UHUGEINT, 'inf'::FLOAT, '-inf'::DOUBLE, -0.01::DECIMAL(9,2), 0::DECIMAL(38,10), '',
                   'it''s', '11111111-2222-3333-4444-555555555555'::UUID, INTERVAL '-3 days', 'happy'::mood, '[]'::JSON,
                   ''::BLOB, DATE '1970-01-01', TIME '00:00:00', TIMESTAMP '1969-12-31 23:59:59.999999',
                   TIMESTAMP_S '1970-01-01 00:00:00', TIMESTAMP_MS '1970-01-01 00:00:00.001', TIMESTAMP_NS '1969-12-31 23:59:59.999999999',
                   TIMESTAMPTZ '1970-01-01 00:00:00+00', [], [NULL, NULL]::INTEGER[2], {'x': NULL, 'y': NULL},
                   MAP {}, [], 0.0::DOUBLE)) t""", 10_000);
    }

    @Test
    void manyRowsAcrossChunksEachAWholeStream() throws Exception {
        Path dir = check("SELECT i, 'r' || i AS s, CASE WHEN i % 7 = 0 THEN NULL ELSE i * 1.5 END AS d"
                + " FROM range(25000) t(i) ORDER BY i", 10_000);
        try (var files = Files.list(dir)) {
            assertEquals(3, files.filter(f -> f.getFileName().toString().startsWith("arrow-")).count(),
                    "25,000 rows at 10,000 a chunk: whole 2,048-row batches, so 10,240 + 10,240 + 4,520");
        }
    }

    /** Runs {@code sql} as JSON and as Arrow, then the comparator over both. The directory it used. */
    static Path check(String sql, int rowsPerChunk) throws Exception {
        String python = python();
        Path dir = Files.createTempDirectory("arrow-vs-json");
        StatementRequest json = new StatementRequest(sql, "main", 60_000, 30_000, rowsPerChunk);
        SqlApiBinding.Done jd = done(WarehouseServerTest.runOn(server, token, json));
        List<List<Json.Node>> rows = WarehouseServerTest.rowsOn(server, token, jd);
        List<Json.Node> types = new ArrayList<>();
        for (var c : jd.status().result().columns()) types.add(Json.str(c.type()));
        LinkedHashMap<String, Json.Node> doc = new LinkedHashMap<>();
        List<Json.Node> rowNodes = new ArrayList<>();
        for (List<Json.Node> r : rows) rowNodes.add(new Json.Arr(r));
        doc.put("rows", new Json.Arr(rowNodes));
        doc.put("types", new Json.Arr(types));
        Files.writeString(dir.resolve("json.json"), Json.toCompact(new Json.Obj(doc)));

        SqlApiBinding.Done ad = done(WarehouseServerTest.runOn(server, token, json.as(ResultFormat.ARROW)));
        Status s = ad.status();
        assertEquals(rows.size(), s.result().rowCount(), "the same rows");
        assertEquals(null, s.firstChunk(), "an Arrow result's chunks are fetched, never inlined");
        for (int i = 0; i < s.result().chunkCount(); i++) {
            HttpResponse<byte[]> r = HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port()
                    + "/sql/v1/statements/" + s.statementId() + "/chunks/" + i))
                    .header("Authorization", "Bearer " + token).build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, r.statusCode());
            assertEquals("application/vnd.apache.arrow.stream", r.headers().firstValue("Content-Type").orElse(""));
            Files.write(dir.resolve("arrow-" + i + ".arrows"), r.body());
        }
        Process p = new ProcessBuilder(python, Path.of("warehouse/src/test/python/arrow_matches_json.py").toString(), dir.toString())
                .redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        assertTrue(p.waitFor(120, TimeUnit.SECONDS), "the comparator did not finish");
        System.out.println(out);
        assertEquals(0, p.exitValue(), out);
        return dir;
    }

    static SqlApiBinding.Done done(SqlApiBinding.Step step) {
        if (step instanceof SqlApiBinding.Failed f) throw new AssertionError(f.error().code() + ": " + f.error().message());
        return (SqlApiBinding.Done) step;
    }

    /** A Python with pyarrow: skipped where there is none, unless the lane requires it. */
    static String python() throws Exception {
        for (String candidate : List.of("python3", "python")) {
            try {
                Process p = new ProcessBuilder(candidate, "-c", "import pyarrow").redirectErrorStream(true).start();
                if (p.waitFor(60, TimeUnit.SECONDS) && p.exitValue() == 0) return candidate;
            } catch (java.io.IOException notHere) {
                // try the next name
            }
        }
        boolean required = "required".equals(System.getenv("WAREHOUSE_ARROW_CHECK"));
        assertTrue(!required, "WAREHOUSE_ARROW_CHECK=required, but no python with pyarrow is on the PATH");
        Assumptions.abort("no python with pyarrow: the Arrow check is SKIPPED here (CI's app lane requires it)");
        throw new IllegalStateException("unreachable");
    }
}
