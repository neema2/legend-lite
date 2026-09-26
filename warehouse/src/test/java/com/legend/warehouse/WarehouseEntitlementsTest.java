package com.legend.warehouse;

import static com.legend.warehouse.WarehouseServerTest.API;
import static com.legend.warehouse.WarehouseServerTest.rowsOn;
import static com.legend.warehouse.WarehouseServerTest.runOn;
import static com.legend.warehouse.WarehouseServerTest.sendTo;
import static com.legend.warehouse.WarehouseServerTest.str;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.server.Json;
import com.legend.warehouse.server.Statements;
import com.legend.warehouse.sqlapi.SqlApi.ErrorCode;
import com.legend.warehouse.sqlapi.SqlApi.ResultFormat;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApiBinding;
import com.legend.warehouse.sqlapi.SqlApiBinding.HttpCall;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * W2 (docs/SERVER_PROGRAM_2026_09_26.md §3): an owner deploys tables, views, table functions and
 * macros and grants SELECT on them; a reader runs one SELECT over what it was granted, and nothing
 * else. The database is locked down for everyone: no files outside the import directory, no URLs,
 * no ATTACH, no extensions.
 */
class WarehouseEntitlementsTest {

    static TestServer server;
    static Path data;
    static String alice;   // owner
    static String carol;   // reader, in role analysts
    static String dave;    // reader

    @BeforeAll
    static void start() throws Exception {
        data = Files.createTempDirectory("warehouse-w2");
        server = TestServer.start(data, List.of(new String[] {"alice", "alice-pw"}, new String[] {"carol", "carol-pw"},
                new String[] {"dave", "dave-pw"}), List.of("alice"),
                new Statements.Limits(2, 50, 1_000_000, Duration.ofMinutes(5)));
        alice = login("alice", "alice-pw");
        carol = login("carol", "carol-pw");
        dave = login("dave", "dave-pw");
        ok(alice, """
                CREATE SCHEMA sales;
                CREATE TABLE sales.orders (id INTEGER, region VARCHAR, owner VARCHAR, amount INTEGER);
                INSERT INTO sales.orders VALUES (1, 'east', 'carol', 10), (2, 'west', 'carol', 20),
                  (3, 'east', 'dave', 30), (4, 'west', 'dave', 40);
                CREATE VIEW sales.v_orders AS SELECT id, region, amount FROM sales.orders;
                CREATE TABLE secret (s VARCHAR);
                INSERT INTO secret VALUES ('the secret');
                CREATE MACRO by_region(r) AS TABLE SELECT id, amount FROM sales.orders WHERE region = r;
                CREATE MACRO double_it(x) AS x * 2;
                CREATE VIEW my_orders AS
                  SELECT id, amount FROM sales.orders WHERE owner = system.main.authenticated_user();
                CREATE SCHEMA pub;
                CREATE TABLE pub.t1 AS SELECT 1 AS a;
                CREATE TABLE pub.t2 AS SELECT 2 AS b;
                CREATE SEQUENCE seq""");
        ok(alice, "CREATE ROLE analysts");
        ok(alice, "GRANT analysts TO carol");
        ok(alice, "GRANT SELECT ON VIEW sales.v_orders TO analysts");
        ok(alice, "GRANT SELECT ON FUNCTION by_region TO carol");
        ok(alice, "GRANT SELECT ON my_orders TO carol");
        ok(alice, "GRANT SELECT ON main.my_orders TO dave");
        ok(alice, "GRANT SELECT ON SCHEMA pub TO dave");
    }

    @AfterAll
    static void stop() throws Exception {
        server.close();
    }

    // -- what a reader may not do ------------------------------------------------------------------

    @Test
    void readersReadOnlyWhatIsGranted() throws Exception {
        for (String sql : List.of(
                "SELECT * FROM secret",
                "SELECT * FROM main.secret",
                "SELECT * FROM main.main.secret",
                "SELECT * FROM sales.orders",
                "SELECT * FROM (SELECT * FROM secret)",
                "WITH x AS (SELECT * FROM secret) SELECT * FROM x",
                // a CTE's own body does not see its name: this 'secret' is the table
                "WITH secret AS (SELECT * FROM secret) SELECT * FROM secret",
                // nor does a recursive CTE's anchor (left), however its unions nest
                "WITH RECURSIVE secret(s) AS (SELECT s FROM secret UNION SELECT 'x' FROM secret) SELECT * FROM secret",
                "WITH RECURSIVE secret(s) AS (SELECT 'a' UNION SELECT s FROM secret UNION SELECT s FROM secret)"
                        + " SELECT * FROM secret",
                "WITH secret AS (SELECT 1 AS i), r(s) AS (WITH RECURSIVE secret(s) AS (SELECT s FROM secret UNION"
                        + " SELECT s FROM secret) SELECT * FROM secret) SELECT * FROM r",
                "WITH v AS (SELECT 1 AS i) SELECT * FROM v, LATERAL (SELECT * FROM secret)",
                "SELECT 'x' UNION ALL SELECT s FROM secret",
                "SELECT (SELECT max(s) FROM secret)",
                "SELECT 1 WHERE EXISTS (SELECT 1 FROM secret)",
                "SELECT * FROM sales.v_orders v JOIN secret ON true",
                "SELECT * FROM system.main.duckdb_tables()",
                "SELECT * FROM duckdb_tables()",
                "SELECT * FROM query('SELECT * FROM secret')",
                "SELECT * FROM query_table('secret')",
                "SELECT * FROM read_csv('/etc/hosts')",
                "SELECT * FROM '/etc/hosts'",
                "SELECT double_it(2)",
                "SELECT main.double_it(2)")) {
            forbidden(carol, sql);
        }
    }

    @Test
    void readersRunOneSelectAndNothingElse() throws Exception {
        for (String sql : List.of(
                "SELECT 1; SELECT 2",
                "SET threads = 1",
                "CREATE TABLE mine (i INTEGER)",
                "INSERT INTO secret VALUES ('x')",
                "DELETE FROM secret",
                "COPY secret TO '/tmp/w2-leak.csv'",
                "ATTACH '/tmp/w2-other.duckdb' AS other",
                "INSTALL httpfs",
                "LOAD httpfs",
                "SHOW TABLES",
                "DESCRIBE secret",
                "PRAGMA table_info('secret')",
                "SELECT current_setting('threads')",
                "SELECT sleep_ms(1)",
                "SELECT nextval('seq')",
                "GRANT SELECT ON secret TO carol",
                "CREATE ROLE mine",
                "GRANT analysts TO dave",
                "SHOW GRANTS")) {
            forbidden(carol, sql);
        }
        // a parse error is a parse error, whoever sends it
        assertEquals(ErrorCode.SQL_PARSE, fail(carol, StatementRequest.of("SELEC 1")));
    }

    @Test
    void describingIsCheckedLikeRunning() throws Exception {
        assertEquals(ErrorCode.FORBIDDEN, fail(carol, describe("SELECT * FROM secret")));
        SqlApiBinding.Step ok = runOn(server, carol, describe("SELECT * FROM sales.v_orders"));
        assertTrue(ok instanceof SqlApiBinding.Done, ok.toString());
    }

    // -- what a reader may do ------------------------------------------------------------------------

    @Test
    void readersQueryGrantedViewsTableFunctionsAndSchemas() throws Exception {
        assertEquals("4", cell(carol, "SELECT count(*) FROM sales.v_orders"));   // through its role
        assertEquals("40", cell(carol, "SELECT sum(amount) FROM by_region('east')"));   // a granted table function
        assertEquals("3", cell(carol, "SELECT count(*) FROM range(3)"));
        assertEquals("6", cell(carol, "SELECT sum(n) FROM generate_series(1, 3) t(n)"));
        assertEquals("100", cell(carol, """
                WITH o AS (SELECT * FROM sales.v_orders)
                SELECT max(t) FROM (SELECT sum(amount) OVER () AS t FROM o)"""));
        assertEquals("60", cell(carol, """
                SELECT west FROM (SELECT region, amount FROM sales.v_orders)
                PIVOT (sum(amount) FOR region IN ('east', 'west'))"""));
        assertEquals("3", cell(carol,
                "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT max(n) FROM r"));
        // the recursive term sees the CTE, however deep: in a subquery, a scalar subquery, EXISTS
        assertEquals("4", cell(carol, """
                WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM (SELECT n FROM r) t
                  WHERE n < (SELECT 4 FROM r LIMIT 1) AND EXISTS (SELECT 1 FROM r)) SELECT max(n) FROM r"""));
        assertEquals("carol|carol", cell(carol, "SELECT authenticated_user() || '|' || current_user"));
        forbidden(carol, "SELECT * FROM pub.t1");
        // a schema grant covers everything in it
        assertEquals("1", cell(dave, "SELECT a FROM pub.t1"));
        assertEquals("2", cell(dave, "SELECT b FROM pub.t2"));
        forbidden(dave, "SELECT * FROM sales.v_orders");
    }

    /** What a reader's SELECT may say, beyond FROM: casts, sorted windows and aggregates, joins, sets. */
    @Test
    void readersWriteOrdinarySql() throws Exception {
        for (String sql : List.of(
                "SELECT CAST(amount AS DECIMAL(10, 2)), [amount]::INTEGER[], {'a': amount}::STRUCT(a INTEGER),"
                        + " CAST('east' AS ENUM('east', 'west')), [1]::INTEGER[1], (1::UNION(a INTEGER))::VARCHAR, region::VARCHAR"
                        + " FROM sales.v_orders",
                "SELECT row_number() OVER (PARTITION BY region ORDER BY amount DESC NULLS LAST"
                        + " ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sales.v_orders ORDER BY 1 ASC",
                "SELECT region, string_agg(id::VARCHAR, ',' ORDER BY id), sum(amount) FILTER (WHERE amount > 10)"
                        + " FROM sales.v_orders GROUP BY region HAVING count(*) > 0 QUALIFY rank() OVER (ORDER BY region) > 0 ORDER BY ALL LIMIT 5",
                "SELECT DISTINCT ON (region) region FROM sales.v_orders LIMIT 50%",
                "SELECT * FROM sales.v_orders USING SAMPLE 100%",
                "SELECT * EXCLUDE (id) REPLACE (amount * 2 AS amount), COLUMNS('reg.*') FROM sales.v_orders",
                "SELECT region FROM sales.v_orders GROUP BY GROUPING SETS ((region), ())",
                "SELECT * FROM range(3) a ASOF JOIN range(3) b ON a.range >= b.range",
                "SELECT * FROM (VALUES (1)) v(a) POSITIONAL JOIN (VALUES (2)) w(b)",
                "SELECT * FROM (SELECT 1 AS a, 2 AS b) UNPIVOT (v FOR k IN (a, b))",
                "SELECT DATE '2020-01-01' + INTERVAL 1 DAY, CASE WHEN 1 = 1 THEN 1 END, 'a' LIKE 'b', 1 IN (1, 2),"
                        + " list_transform([1], x -> x + 1), MAP {'a': 1}, TRY_CAST('1' AS INTEGER)",
                "SELECT unnest([1, 2]), generate_series(1, 2)",
                "SELECT a.id FROM sales.v_orders a JOIN sales.v_orders b USING (id) WHERE a.id IN"
                        + " (SELECT id FROM sales.v_orders) INTERSECT SELECT id FROM sales.v_orders",
                "FROM sales.v_orders SELECT id",
                "TABLE sales.v_orders")) {
            SqlApiBinding.Step step = runOn(server, carol, StatementRequest.of(sql));
            if (step instanceof SqlApiBinding.Failed f) {
                throw new AssertionError(sql + " -> " + f.error().code() + ": " + f.error().message());
            }
        }
    }

    @Test
    void aViewCanFilterRowsByWhoIsAsking() throws Exception {
        assertEquals("30", cell(carol, "SELECT sum(amount) FROM my_orders"));
        assertEquals("70", cell(dave, "SELECT sum(amount) FROM my_orders"));
        assertEquals("0", cell(alice, "SELECT count(*) FROM my_orders"));
    }

    @Test
    void grantsComeAndGo() throws Exception {
        ok(alice, "CREATE TABLE temporary_grant AS SELECT 7 AS n");
        forbidden(dave, "SELECT n FROM temporary_grant");
        ok(alice, "GRANT SELECT ON TABLE temporary_grant TO dave");
        assertEquals("7", cell(dave, "SELECT n FROM temporary_grant"));
        ok(alice, "REVOKE SELECT ON TABLE temporary_grant FROM dave");
        forbidden(dave, "SELECT n FROM temporary_grant");

        ok(alice, "CREATE ROLE temps");
        ok(alice, "GRANT SELECT ON temporary_grant TO temps");
        ok(alice, "GRANT temps TO dave");
        assertEquals("7", cell(dave, "SELECT n FROM temporary_grant"));
        ok(alice, "DROP ROLE temps");
        forbidden(dave, "SELECT n FROM temporary_grant");
    }

    @Test
    void ownersSeeTheGrants() throws Exception {
        List<List<Json.Node>> rows = query(alice, StatementRequest.of("SHOW GRANTS"));
        Set<String> seen = new TreeSet<>();
        for (List<Json.Node> r : rows) seen.add(str(r.get(0)) + "." + str(r.get(1)) + "." + str(r.get(2)) + " " + str(r.get(3)));
        assertTrue(seen.contains("main.sales.v_orders analysts"), seen.toString());
        assertTrue(seen.contains("main.pub. dave"), seen.toString());
        // and in Arrow, like any result
        assertTrue(runOn(server, alice, new StatementRequest("SHOW GRANTS", "main", 30_000, 30_000, 1_000, null, false,
                ResultFormat.ARROW, false)) instanceof SqlApiBinding.Done);
    }

    // -- the lockdown, for owners too ------------------------------------------------------------------

    @Test
    void ownersLoadFromTheImportDirectoryOnly() throws Exception {
        Path csv = data.resolve("import").resolve("people.csv");
        Files.writeString(csv, "name,age\nann,31\nbo,42\n");
        assertEquals("73", cell(alice, "SELECT sum(age) FROM read_csv('" + csv + "')"));
        Path outside = Files.createTempFile("warehouse-w2-outside", ".csv");
        Files.writeString(outside, "a\n1\n");
        for (String sql : List.of(
                "SELECT * FROM read_csv('" + outside + "')",
                "COPY secret TO '" + outside + "'",
                "ATTACH '" + data.resolve("elsewhere.duckdb") + "' AS elsewhere",
                "INSTALL httpfs",
                "SET enable_external_access = true")) {
            SqlApiBinding.Step step = runOn(server, alice, StatementRequest.of(sql));
            assertTrue(step instanceof SqlApiBinding.Failed, sql + " ran for an owner");
        }
        // the grants live in the system database, which no catalog can reach
        SqlApiBinding.Step step = runOn(server, alice, StatementRequest.of("SELECT * FROM system.main.security_grants"));
        assertTrue(step instanceof SqlApiBinding.Failed, "an owner read the grants table");
    }

    @Test
    void theCatalogListsWhatTheCallerMayRead() throws Exception {
        assertEquals(Set.of("main.my_orders", "sales.v_orders"), objects(carol));
        assertEquals(Set.of("main.my_orders", "pub.t1", "pub.t2"), objects(dave));
        Set<String> all = objects(alice);
        assertTrue(all.containsAll(List.of("main.secret", "sales.orders", "sales.v_orders", "pub.t1")), all.toString());
    }

    // -- helpers ---------------------------------------------------------------------------------------

    static String login(String user, String password) throws Exception {
        return API.token(sendTo(server, API.login(user, password))).token();
    }

    static StatementRequest describe(String sql) {
        return new StatementRequest(sql, "main", 30_000, 30_000, 1_000, null, true, ResultFormat.JSON, false);
    }

    static void ok(String token, String sql) throws Exception {
        query(token, StatementRequest.of(sql));
    }

    static List<List<Json.Node>> query(String token, StatementRequest req) throws Exception {
        SqlApiBinding.Step step = runOn(server, token, req);
        if (step instanceof SqlApiBinding.Failed f) {
            throw new AssertionError(req.sql() + " -> " + f.error().code() + ": " + f.error().message());
        }
        return rowsOn(server, token, (SqlApiBinding.Done) step);
    }

    static String cell(String token, String sql) throws Exception {
        Json.Node n = query(token, StatementRequest.of(sql)).get(0).get(0);
        return n instanceof Json.Str s ? s.value() : Json.toCompact(n);
    }

    static ErrorCode fail(String token, StatementRequest req) throws Exception {
        SqlApiBinding.Step step = runOn(server, token, req);
        if (!(step instanceof SqlApiBinding.Failed f)) throw new AssertionError(req.sql() + " was allowed");
        return f.error().code();
    }

    static void forbidden(String token, String sql) throws Exception {
        SqlApiBinding.Step step = runOn(server, token, StatementRequest.of(sql));
        if (!(step instanceof SqlApiBinding.Failed f)) throw new AssertionError(sql + " was allowed");
        assertEquals(ErrorCode.FORBIDDEN, f.error().code(), sql + ": " + f.error().message());
        assertFalse(f.error().message().contains("the secret"), sql);
    }

    static Set<String> objects(String token) throws Exception {
        HttpCall call = new HttpCall("GET", "/sql/v1/catalogs/main/objects",
                Map.of("Authorization", "Bearer " + token), null);
        SqlApiBinding.HttpResult r = sendTo(server, call);
        assertEquals(200, r.status(), r.body());
        Set<String> out = new TreeSet<>();
        List<String> seen = new ArrayList<>();
        for (Json.Node n : ((Json.Arr) Json.parse(r.body())).items()) {
            Json.Obj o = (Json.Obj) n;
            seen.add(str(o.get("schema")) + "." + str(o.get("name")));
        }
        out.addAll(seen);
        return out;
    }
}
