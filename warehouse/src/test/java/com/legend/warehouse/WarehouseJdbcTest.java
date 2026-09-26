package com.legend.warehouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.warehouse.server.Statements;
import com.legend.warehouse.server.WarehouseServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * THE DIFFERENTIAL: the same SQL through the warehouse's JDBC driver (over
 * HTTP, through the warehouse) and through DuckDB's own driver in-process,
 * compared value by value -- class, printed form, type name, JDBC type
 * code, nested elements. legend-lite's executor is written against DuckDB's
 * driver's objects, so this is what lets it run on the warehouse unchanged.
 *
 * <p>Two NAMED differences, where the class is DuckDB's own internals: a
 * BLOB is compared by its bytes, and a JSON value by its text (DuckDB hands
 * back its own {@code org.duckdb.JsonNode}; this driver, the text itself).
 */
class WarehouseJdbcTest {

    static WarehouseServer server;
    static Connection remote;
    static Connection local;

    @BeforeAll
    static void start() throws Exception {
        Path data = Files.createTempDirectory("warehouse-jdbc");
        server = new WarehouseServer(new WarehouseServer.Config(0, data, List.of("main"),
                List.<String[]>of(new String[] {"alice", "alice-pw"}), null, Duration.ofMinutes(5),
                new Statements.Limits(2, 50, 1_000_000, Duration.ofMinutes(5))));
        remote = DriverManager.getConnection("jdbc:warehouse:http://127.0.0.1:" + server.port()
                + "/main?user=alice&password=alice-pw");
        local = DriverManager.getConnection("jdbc:duckdb:");
        for (Connection c : List.of(remote, local)) {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
            }
        }
    }

    @AfterAll
    static void stop() throws Exception {
        remote.close();
        local.close();
        server.close();
    }

    /** A value's comparable form: its class kind and printed form, all the way down. */
    static String shape(Object v, String typeName) throws SQLException {
        if (v == null) return "null";
        if (typeName.equals("JSON")) return "JSON:" + v;                          // named difference
        if (v instanceof Blob b) return "BLOB:" + HexFormat.of().formatHex(b.getBytes(1, (int) b.length()));
        if (v instanceof Array a) {
            List<String> items = new ArrayList<>();
            for (Object o : (Object[]) a.getArray()) items.add(shape(o, a.getBaseTypeName()));
            return "Array<" + a.getBaseTypeName() + "," + a.getBaseType() + ">" + items + " printed " + a;
        }
        if (v instanceof Struct s) {
            List<String> attrs = new ArrayList<>();
            for (Object o : s.getAttributes()) attrs.add(shape(o, ""));
            return "Struct<" + s.getSQLTypeName() + ">" + attrs + " printed " + s;
        }
        if (v instanceof Map<?, ?> m) {
            List<String> es = new ArrayList<>();
            for (Map.Entry<?, ?> e : m.entrySet()) es.add(shape(e.getKey(), "") + "=" + shape(e.getValue(), ""));
            return v.getClass().getName() + es;
        }
        return v.getClass().getName() + ":" + v;
    }

    static List<String> describe(Connection c, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String type = md.getColumnTypeName(i);
                    String cell = md.getColumnLabel(i) + " " + type + " jdbc=" + md.getColumnType(i)
                            + " -> " + shape(rs.getObject(i), type);
                    if (type.startsWith("TIMESTAMP") && !type.contains("ZONE") && !type.endsWith("]")) {
                        cell += " asLocal=" + rs.getObject(i, LocalDateTime.class);
                    }
                    if (!type.equals("BLOB") && !type.equals("JSON")) {
                        cell += " string=" + rs.getString(i) + " null=" + rs.wasNull();
                    }
                    out.add(cell);
                }
            }
        }
        return out;
    }

    static void same(String sql) throws SQLException {
        List<String> want = describe(local, sql);
        List<String> got = describe(remote, sql);
        assertEquals(want.size(), got.size(), "row/column count differs for: " + sql);
        for (int i = 0; i < want.size(); i++) assertEquals(want.get(i), got.get(i), "cell " + i + " of: " + sql);
    }

    @Test
    void everyScalarTypeComesBackAsDuckDBsOwnDriverGivesIt() throws SQLException {
        same("""
                SELECT 1::TINYINT a, 2::SMALLINT b, 3::INTEGER c, 4::BIGINT d, 5::HUGEINT e, 6::UBIGINT f,
                       7::UTINYINT g, 8::USMALLINT h, 9::UINTEGER i, 10::UHUGEINT j,
                       1.5::FLOAT k, 2.5::DOUBLE l, 3.25::DECIMAL(10,2) m, 1.5::DECIMAL(38,10) n,
                       'x' o, DATE '2024-01-02' p, TIME '01:02:03' q, TIMESTAMP '2024-01-02 03:04:05' r,
                       TIMESTAMP_S '2024-01-02 03:04:05' s, TIMESTAMP_MS '2024-01-02 03:04:05.123' t,
                       TIMESTAMP_NS '2024-01-02 03:04:05.123456789' u, TIMESTAMPTZ '2024-01-02 03:04:05+00' v,
                       true w, '\\x41\\x00B'::BLOB x, '00000000-0000-0000-0000-000000000042'::UUID y,
                       INTERVAL 3 DAY z, 'ok'::mood aa, '{"a":[1,2]}'::JSON ab""");
    }

    @Test
    void edgeValuesSurviveTheTrip() throws SQLException {
        same("""
                SELECT -0.0::DOUBLE a, 'nan'::DOUBLE b, 'inf'::FLOAT c, '-inf'::DOUBLE d,
                       9223372036854775807::BIGINT e, -170141183460469231731687303715884105727::HUGEINT f,
                       123456789012345678901234567890.12345678::DECIMAL(38,8) g, '' h, 'quote " and \\ back' i,
                       DATE '1970-01-01' j, TIMESTAMP '2024-02-29 00:00:00' k, TIME '00:00:00' l,
                       0.1::FLOAT m""");
    }

    @Test
    void nullsOfEveryKind() throws SQLException {
        same("""
                SELECT NULL::INTEGER a, NULL::BIGINT b, NULL::VARCHAR c, NULL::DATE d, NULL::TIMESTAMP e,
                       NULL::DECIMAL(10,2) f, NULL::BLOB g, NULL::INTEGER[] h, NULL::STRUCT(x INTEGER) i,
                       NULL::BOOLEAN j, NULL::DOUBLE k""");
    }

    @Test
    void nestedTypesComeBackAsArraysStructsAndMaps() throws SQLException {
        same("""
                SELECT [1,2,3]::INTEGER[3] a, [{'x': 1, 'y': [1.5, NULL]}] b, {'a': [1,2], 'b': {'c': 'd'}} c,
                       MAP {1: [1,2]} d, [DATE '2024-01-01'] e, [TIMESTAMP '2024-01-01 00:00:00'] f,
                       ['a', NULL] g, [[1],[2,3]] h, []::INTEGER[] i, {'k': NULL}::STRUCT(k INTEGER) j""");
    }

    @Test
    void manyRowsAcrossChunks() throws SQLException {
        same("SELECT i, i * 1.5 AS d, 'r' || i AS s FROM range(25000) t(i) ORDER BY i");
    }

    @Test
    void writesAndDdlReportUpdateCountsAsDuckDBDoes() throws SQLException {
        for (Connection c : List.of(remote, local)) {
            try (Statement s = c.createStatement()) {
                assertFalse(s.execute("CREATE OR REPLACE TABLE notes (id INTEGER, note VARCHAR)"));
                long ddl = s.getUpdateCount();
                assertFalse(s.execute("INSERT INTO notes VALUES (1, 'a'), (2, 'b'), (3, 'c')"));
                assertEquals(3, s.getUpdateCount(), "insert count on " + c);
                assertEquals(-1, ddl, "DDL update count on " + c);
            }
        }
        same("SELECT * FROM notes ORDER BY id");
    }

    @Test
    void errorsCarryDuckDBsMessage() {
        SQLException e = assertThrows(SQLException.class,
                () -> remote.createStatement().executeQuery("SELECT * FROM no_such_table"));
        assertTrue(e.getMessage().contains("no_such_table"), e.getMessage());
        assertEquals("SQL_BIND", e.getSQLState());
    }

    static String one(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row: " + sql);
            return rs.getString(1);
        }
    }

    static Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:warehouse:http://127.0.0.1:" + server.port()
                + "/main?user=alice&password=alice-pw");
    }

    @Test
    void aConnectionIsASessionWhatAStatementLeavesIsThereForTheNext() throws SQLException {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("SET TimeZone = 'America/New_York'");
            assertEquals("America/New_York", one(c, "SELECT current_setting('TimeZone')"));
            // The corpus harness's workspace, exactly: attach, use, work.
            String home = one(c, "SELECT current_database()");
            s.execute("ATTACH ':memory:' AS ws_probe");
            s.execute("USE ws_probe");
            s.execute("CREATE TABLE fixture AS SELECT 42 AS answer");
            assertEquals("42", one(c, "SELECT answer FROM fixture"));
            s.execute("CREATE TEMP TABLE scratch AS SELECT 'mine' AS who");
            assertEquals("mine", one(c, "SELECT who FROM scratch"));
            s.execute("USE \"" + home + "\"");
            s.execute("DETACH ws_probe");
        }
    }

    @Test
    void twoConnectionsDoNotSeeEachOthersSessions() throws SQLException {
        try (Connection a = connect(); Connection b = connect()) {
            a.createStatement().execute("CREATE TEMP TABLE only_a AS SELECT 1 AS x");
            assertEquals("1", one(a, "SELECT x FROM only_a"));
            SQLException e = assertThrows(SQLException.class, () -> one(b, "SELECT x FROM only_a"));
            assertTrue(e.getMessage().contains("only_a"), e.getMessage());
        }
    }

    @Test
    void transactionsRollBackAndCommit() throws SQLException {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("CREATE OR REPLACE TABLE ledger (id INTEGER)");
            c.setAutoCommit(false);
            s.execute("INSERT INTO ledger VALUES (1), (2)");
            c.rollback();
            assertEquals("0", one(c, "SELECT count(*) FROM ledger"));
            s.execute("INSERT INTO ledger VALUES (3)");
            c.commit();
            c.setAutoCommit(true);
        }
        try (Connection other = connect()) {
            assertEquals("1", one(other, "SELECT count(*) FROM ledger"), "the commit is visible elsewhere");
        }
    }

    @Test
    void aScriptRunsAndAnswersWithItsLastStatement() throws SQLException {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertTrue(s.execute("CREATE OR REPLACE TABLE s1 (x INTEGER); INSERT INTO s1 VALUES (1), (2); "
                    + "SELECT sum(x) FROM s1"));
            ResultSet rs = s.getResultSet();
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
            SQLException e = assertThrows(SQLException.class, () -> s.execute("SELECT 1; SELECT * FROM nope_second"));
            assertTrue(e.getMessage().contains("nope_second"), "the real error, whichever statement: " + e.getMessage());
        }
    }

    @Test
    void anEmptyResultHasItsColumnsAndNoRows() throws SQLException {
        try (Statement s = remote.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1 AS x WHERE false")) {
            assertEquals(1, rs.getMetaData().getColumnCount());
            assertEquals("INTEGER", rs.getMetaData().getColumnTypeName(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void aPreparedStatementDescribesItsColumnsBeforeRunningAsDuckDBDoes() throws SQLException {
        String sql = """
                SELECT true a, 1::TINYINT b, 1::SMALLINT c, 1::INTEGER d, 1::BIGINT e, 1::HUGEINT f,
                       1::UTINYINT g, 1::USMALLINT h, 1::UINTEGER i, 1::UBIGINT j, 1::UHUGEINT k,
                       1::FLOAT l, NULL::DOUBLE m, 2.5::DECIMAL(9,2) n, 1.5::DECIMAL(38,10) o, 'x' p,
                       uuid() q, INTERVAL 1 DAY r, DATE '2020-01-01' s, TIME '01:02:03' t,
                       TIMESTAMP '2020-01-01' u, TIMESTAMP_S '2020-01-01' v, TIMESTAMP_MS '2020-01-01' w,
                       TIMESTAMP_NS '2020-01-01' x, TIMESTAMPTZ '2020-01-01' y, 'a'::BLOB z,
                       '{}'::JSON aa, [1, 2] ab, [1, 2]::INTEGER[2] ac, {'k': 1} ad, MAP {1: 2} ae,
                       'a'::ENUM('a', 'b') af""";
        try (var r = remote.prepareStatement(sql); var l = local.prepareStatement(sql)) {
            java.sql.ResultSetMetaData rm = r.getMetaData();
            java.sql.ResultSetMetaData lm = l.getMetaData();
            assertEquals(lm.getColumnCount(), rm.getColumnCount());
            for (int i = 1; i <= lm.getColumnCount(); i++) {
                assertEquals(lm.getColumnLabel(i), rm.getColumnLabel(i));
                assertEquals(lm.getColumnTypeName(i), rm.getColumnTypeName(i));
                assertEquals(lm.getColumnType(i), rm.getColumnType(i), lm.getColumnTypeName(i));
                assertEquals(lm.getPrecision(i), rm.getPrecision(i), lm.getColumnTypeName(i));
                assertEquals(lm.getScale(i), rm.getScale(i), lm.getColumnTypeName(i));
                assertEquals(lm.isNullable(i), rm.isNullable(i), lm.getColumnTypeName(i));
                assertEquals(lm.isSigned(i), rm.isSigned(i), lm.getColumnTypeName(i));
                // DuckDB names its own classes for its array, struct, blob and
                // JSON node; this driver names the JDBC interface (or String)
                String cls = lm.getColumnClassName(i);
                if (cls.startsWith("java.")) {
                    assertEquals(cls, rm.getColumnClassName(i), lm.getColumnTypeName(i));
                }
            }
        }
    }

    @Test
    void describingDoesNotRunTheStatement() throws SQLException {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE described (x INTEGER)");
            try (var p = c.prepareStatement("INSERT INTO described VALUES (1) RETURNING x")) {
                assertEquals(1, p.getMetaData().getColumnCount());
            }
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM described")) {
                assertTrue(rs.next());
                assertEquals(0L, rs.getLong(1));
            }
        }
    }

    @Test
    void theDatabaseIsTheEngineBehindTheSession() throws SQLException {
        assertEquals(local.getMetaData().getDatabaseProductName(), remote.getMetaData().getDatabaseProductName());
        assertEquals(local.getMetaData().getDatabaseProductVersion(), remote.getMetaData().getDatabaseProductVersion());
    }
}
