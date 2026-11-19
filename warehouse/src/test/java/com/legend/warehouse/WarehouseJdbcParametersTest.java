package com.legend.warehouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.warehouse.server.Statements;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A PREPARED STATEMENT'S PARAMETERS through the warehouse's JDBC driver (track Q2, 2026-11-19): what legend-lite's
 * exec seam binds ({@code JdbcStatements.bind}: {@code setString}, {@code setObject} and {@code setNull}) carried to
 * the warehouse as the API's values and bound there, never written into the text -- each judged against DuckDB's own
 * driver in-process, the same calls on both, compared cell by cell (the result's type and value, as
 * {@link WarehouseJdbcTest}'s differential compares them). A value the warehouse does not bind is refused by name.
 */
class WarehouseJdbcParametersTest {

    static TestServer server;
    /** The warehouse's data directory: its {@code import} folder is where it reads a file from (Database.lockDown). */
    static Path data;
    static Connection remote;
    static Connection local;

    /** A setter applied to a prepared statement at an index. */
    @FunctionalInterface
    interface Setter {
        void set(PreparedStatement st, int index) throws SQLException;
    }

    @BeforeAll
    static void start() throws Exception {
        data = Files.createTempDirectory("warehouse-jdbc-parameters");
        server = TestServer.start(data, List.<String[]>of(new String[] {"alice", "alice-pw"}),
                new Statements.Limits(2, 50, 1_000_000, Duration.ofMinutes(5)));
        remote = connect();
        local = DriverManager.getConnection("jdbc:duckdb:");
        // DuckDB's own driver in the session every warehouse connection starts in (WarehouseJdbcTest.start)
        try (Statement s = local.createStatement()) {
            s.execute("SET SESSION TimeZone = 'UTC'");
        }
        // the tables the writes under test write into: one setup script (the main catalog's, every session sees them)
        try (Statement s = remote.createStatement()) {
            s.execute("CREATE TABLE victims (name VARCHAR); CREATE TABLE written (i BIGINT, d DOUBLE, b BOOLEAN,"
                    + " t VARCHAR); CREATE TABLE counted (x INTEGER, y VARCHAR); CREATE TABLE loaded (id BIGINT,"
                    + " name VARCHAR)");
        }
    }

    @AfterAll
    static void stop() throws Exception {
        remote.close();
        local.close();
        server.close();
    }

    static Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:warehouse:http://127.0.0.1:" + server.port()
                + "/main?user=alice&password=alice-pw");
    }

    /** Every cell of {@code sql} on {@code c}, its parameters {@code 1..n} each set by {@code setter}: its label, type
     *  name, JDBC type, value's shape and text -- or the failure's class, when it fails. */
    static List<String> cells(Connection c, String sql, int n, Setter setter) throws SQLException {
        List<String> out = new ArrayList<>();
        try (PreparedStatement st = c.prepareStatement(sql)) {
            for (int i = 1; i <= n; i++) setter.set(st, i);
            try (ResultSet rs = st.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                while (rs.next()) {
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        String type = md.getColumnTypeName(i);
                        out.add(md.getColumnLabel(i) + " " + type + " jdbc=" + md.getColumnType(i) + " -> "
                                + WarehouseJdbcTest.shape(rs.getObject(i), type) + " string=" + rs.getString(i)
                                + " null=" + rs.wasNull());
                    }
                }
            }
        }
        return out;
    }

    /** {@code sql} bound by {@code setter} answers on the warehouse exactly as on DuckDB's own driver. */
    static void same(String sql, int n, Setter setter) throws SQLException {
        assertEquals(cells(local, sql, n, setter), cells(remote, sql, n, setter), sql);
    }

    /** The bound value's own type and its text, as DuckDB binds it: {@code typeof} and a VARCHAR cast of each. */
    static void typedAlike(Setter setter) throws SQLException {
        same("SELECT ? AS v", 1, setter);
        same("SELECT typeof(?) AS t, CAST(? AS VARCHAR) AS s", 2, setter);
    }

    @Test
    void setStringBindsAVarcharAsDuckDBsOwnDriverDoes() throws SQLException {
        typedAlike((st, i) -> st.setString(i, "plain"));
        typedAlike((st, i) -> st.setString(i, ""));
        typedAlike((st, i) -> st.setString(i, "ünïcødé ✓ 𝄞"));
        typedAlike((st, i) -> st.setString(i, null));
    }

    @Test
    void setObjectBindsEachValueTheWarehouseCarriesAsDuckDBsOwnDriverDoes() throws SQLException {
        typedAlike((st, i) -> st.setObject(i, "text"));
        typedAlike((st, i) -> st.setObject(i, 42L));
        typedAlike((st, i) -> st.setObject(i, Long.MIN_VALUE));
        typedAlike((st, i) -> st.setObject(i, Long.MAX_VALUE));
        typedAlike((st, i) -> st.setObject(i, 2.5d));
        // a whole double is a DOUBLE, never a BIGINT: its JSON keeps the point (ApiJson.doubleJson)
        typedAlike((st, i) -> st.setObject(i, 2.0d));
        typedAlike((st, i) -> st.setObject(i, -0.0d));
        typedAlike((st, i) -> st.setObject(i, 1e18d));
        typedAlike((st, i) -> st.setObject(i, 1e-7d));
        typedAlike((st, i) -> st.setObject(i, Double.MIN_VALUE));
        typedAlike((st, i) -> st.setObject(i, 1e300d));
        typedAlike((st, i) -> st.setObject(i, true));
        typedAlike((st, i) -> st.setObject(i, false));
        typedAlike((st, i) -> st.setObject(i, null));
    }

    @Test
    void setNullBindsAnUntypedNullAsDuckDBsOwnDriverDoesWhateverItsTypeSays() throws SQLException {
        for (int type : new int[] {Types.INTEGER, Types.BIGINT, Types.VARCHAR, Types.DOUBLE, Types.DECIMAL,
                Types.BOOLEAN, Types.DATE, Types.TIMESTAMP, Types.NULL}) {
            typedAlike((st, i) -> st.setNull(i, type));
        }
    }

    @Test
    void aParameterTakesTheTypeItsPlaceAsksForAsOnDuckDBsOwnDriver() throws SQLException {
        // the shapes the exec seam sends: a value compared with a column, a date-time as its text in its declared cast
        // (DuckDb.parameterType: CAST(? AS TIMESTAMP), bound by setString), a number in arithmetic
        same("SELECT x FROM (VALUES (1::INTEGER), (2), (3)) t(x) WHERE x = ? ORDER BY x", 1, (st, i) -> st.setObject(i, 2L));
        same("SELECT x FROM (VALUES ('a'), ('b')) t(x) WHERE x <> ? ORDER BY x", 1, (st, i) -> st.setObject(i, "a"));
        same("SELECT CAST(? AS TIMESTAMP) AS t", 1, (st, i) -> st.setString(i, "2020-01-02 03:04:05.123456"));
        same("SELECT CAST(? AS TIMESTAMP) AS t", 1, (st, i) -> st.setString(i, "2020-01-02"));
        same("SELECT ? + 1 AS n, ? * 2 AS d", 2, (st, i) -> st.setObject(i, i == 1 ? (Object) 41L : (Object) 1.25d));
        same("SELECT CASE WHEN ? THEN 'yes' ELSE 'no' END AS b", 1, (st, i) -> st.setObject(i, true));
    }

    @Test
    void aDataPayloadIsBoundAsItsOneTextAndSplitByTheDatabase() throws SQLException {
        // DuckDb.sourceUrl's statement for a data: payload, bound as JdbcStatements binds a Binding.Payload (setString)
        String payload = "[{\"id\":1,\"name\":\"it's\"},{\"id\":2,\"name\":\"a \\\"quote\\\"\"},{\"id\":3,\"name\":null}]";
        same("SELECT CAST(legend_payload.key AS BIGINT) AS place, legend_payload.value AS row"
                + " FROM json_each(CAST(? AS JSON)) AS legend_payload ORDER BY place", 1,
                (st, i) -> st.setString(i, payload));
    }

    @Test
    void aValueIsBoundNeverWrittenIntoTheText() throws SQLException {
        String hostile = "x'); DROP TABLE victims; --";
        try (Connection c = connect(); Statement s = c.createStatement()) {
            try (PreparedStatement p = c.prepareStatement("INSERT INTO victims VALUES (?)")) {
                p.setString(1, hostile);
                assertEquals(1, p.executeUpdate());
            }
            try (ResultSet rs = s.executeQuery("SELECT name FROM victims")) {
                assertTrue(rs.next());
                assertEquals(hostile, rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void writesBindTheirValuesAndReportTheirCounts() throws SQLException {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            try (PreparedStatement p = c.prepareStatement("INSERT INTO written VALUES (?, ?, ?, ?)")) {
                p.setObject(1, 7L);
                p.setObject(2, 0.5d);
                p.setObject(3, true);
                p.setString(4, "seven");
                assertEquals(1, p.executeUpdate());
                // the values stay set until cleared or replaced (JDBC): the statement runs again with them
                p.setNull(4, Types.VARCHAR);
                assertFalse(p.execute());
                assertEquals(1, p.getUpdateCount());
            }
            try (ResultSet rs = s.executeQuery("SELECT i, d, b, t FROM written ORDER BY t NULLS LAST")) {
                assertTrue(rs.next());
                assertEquals(List.of(7L, 0.5d, true, "seven"),
                        List.of(rs.getObject(1), rs.getObject(2), rs.getObject(3), rs.getObject(4)));
                assertTrue(rs.next());
                assertEquals(7L, rs.getLong(1));
                assertEquals(null, rs.getObject(4));
                assertFalse(rs.next());
            }
            try (PreparedStatement p = c.prepareStatement("DELETE FROM written WHERE i = ?")) {
                p.setObject(1, 7L);
                assertEquals(2, p.executeUpdate());
            }
        }
    }

    @Test
    void aStatementDescribesItsColumnsBeforeItsValuesAreSetAsDuckDBDoes() throws SQLException {
        String sql = "SELECT CAST(? AS INTEGER) AS n, CAST(? AS VARCHAR) AS s";
        try (PreparedStatement r = remote.prepareStatement(sql); PreparedStatement l = local.prepareStatement(sql)) {
            ResultSetMetaData rm = r.getMetaData();
            ResultSetMetaData lm = l.getMetaData();
            assertEquals(lm.getColumnCount(), rm.getColumnCount());
            for (int i = 1; i <= lm.getColumnCount(); i++) {
                assertEquals(lm.getColumnLabel(i), rm.getColumnLabel(i));
                assertEquals(lm.getColumnTypeName(i), rm.getColumnTypeName(i));
                assertEquals(lm.getColumnType(i), rm.getColumnType(i));
            }
        }
    }

    @Test
    void aValueTheWarehouseDoesNotBindIsRefusedByNameAtItsSetter() throws SQLException {
        try (Connection c = connect()) {
            try (PreparedStatement p = c.prepareStatement("SELECT CAST(? AS VARCHAR) AS v")) {
                for (Object v : new Object[] {new java.math.BigDecimal("1.50"), 1, (short) 1, 1.5f,
                        java.time.LocalDate.of(2020, 1, 2), java.time.LocalDateTime.of(2020, 1, 2, 3, 4),
                        java.sql.Date.valueOf("2020-01-02"), new byte[] {1}}) {
                    SQLFeatureNotSupportedException e = assertThrows(SQLFeatureNotSupportedException.class,
                            () -> p.setObject(1, v));
                    assertTrue(e.getMessage().contains(v.getClass().getName()), e.getMessage());
                }
                for (double d : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
                    SQLFeatureNotSupportedException e = assertThrows(SQLFeatureNotSupportedException.class,
                            () -> p.setObject(1, d));
                    assertTrue(e.getMessage().contains(String.valueOf(d)), e.getMessage());
                }
                // the setters the exec seam does not call stay refused, loudly
                assertThrows(SQLFeatureNotSupportedException.class, () -> p.setInt(1, 1));
                assertThrows(SQLFeatureNotSupportedException.class, () -> p.setBigDecimal(1, java.math.BigDecimal.ONE));
                assertThrows(SQLFeatureNotSupportedException.class, () -> p.setObject(1, "x", Types.VARCHAR));
                assertThrows(SQLFeatureNotSupportedException.class, () -> p.setArray(1, null));
            }
        }
    }

    @Test
    void anIndexLeftUnsetIsRefusedByNameNeverSentAsANull() throws SQLException {
        try (Connection c = connect();
                PreparedStatement p = c.prepareStatement("SELECT CAST(? AS VARCHAR) AS a, CAST(? AS VARCHAR) AS b")) {
            p.setString(2, "b");
            // the driver's own words: refused before anything is sent (the warehouse would name a missing value its way)
            SQLException e = assertThrows(SQLException.class, p::executeQuery);
            assertTrue(e.getMessage().contains("parameter 1 was not set"), e.getMessage());
            assertThrows(SQLException.class, () -> p.setString(0, "zero"));
        }
    }

    @Test
    void clearParametersForgetsTheValuesAsDuckDBsOwnDriverDoes() throws SQLException {
        for (Connection c : List.of(remote, local)) {
            try (PreparedStatement p = c.prepareStatement("SELECT CAST(? AS VARCHAR) AS v")) {
                p.setString(1, "kept");
                try (ResultSet rs = p.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("kept", rs.getString(1));
                }
                p.clearParameters();
                // no value for the statement's one parameter: the database refuses it, on both drivers
                assertThrows(SQLException.class, p::executeQuery, String.valueOf(c));
            }
        }
    }

    @Test
    void aStatementWhoseResultWaitsOnItsValuesIsDescribedAsDuckDBsOwnDriverDescribesIt() throws SQLException {
        // DuckDB's placeholder column (one 'unknown' of type UNKNOWN, JDBC OTHER) for a result typed only once its
        // values are bound -- exec's own load among them -- every field held to DuckDB's own driver's
        for (String sql : new String[] {"SELECT ?", "SELECT ? AS a, ? AS b", "SELECT ? + 1 AS n",
                "SELECT * FROM read_csv(?, header = true, auto_detect = false, columns = {'c0': 'VARCHAR'})"}) {
            try (PreparedStatement r = remote.prepareStatement(sql); PreparedStatement l = local.prepareStatement(sql)) {
                ResultSetMetaData rm = r.getMetaData();
                ResultSetMetaData lm = l.getMetaData();
                assertEquals(lm.getColumnCount(), rm.getColumnCount(), sql);
                for (int i = 1; i <= lm.getColumnCount(); i++) {
                    assertEquals(lm.getColumnLabel(i), rm.getColumnLabel(i), sql);
                    assertEquals(lm.getColumnName(i), rm.getColumnName(i), sql);
                    assertEquals(lm.getColumnTypeName(i), rm.getColumnTypeName(i), sql);
                    assertEquals(lm.getColumnType(i), rm.getColumnType(i), sql);
                    assertEquals(lm.getPrecision(i), rm.getPrecision(i), sql);
                    assertEquals(lm.getScale(i), rm.getScale(i), sql);
                    assertEquals(lm.isNullable(i), rm.isNullable(i), sql);
                    assertEquals(lm.isSigned(i), rm.isSigned(i), sql);
                    assertEquals(lm.getColumnClassName(i), rm.getColumnClassName(i), sql);
                }
            }
        }
    }

    @Test
    void theParameterCountIsDuckDBsOwnAsItsOwnDriverReportsIt() throws SQLException {
        for (String sql : new String[] {
                "SELECT 1",
                "SELECT ?",
                "SELECT ? AS a, ? AS b, ? AS c",
                "SELECT '?' AS not_a_parameter, ? AS one -- ? in a comment",
                "SELECT $1 AS a, $1 AS again, $2 AS b",
                "SELECT x FROM (VALUES (1), (2)) t(x) WHERE x IN (?, ?)",
                // the exec seam's load of a file (JdbcStatements.readsFile): one value, the admitted path
                "SELECT * FROM read_csv(?, header = true, delim = ',', quote = '\"', escape = '\"', auto_detect = false,"
                        + " strict_mode = true, nullstr = '', columns = {'c0': 'VARCHAR', 'c1': 'VARCHAR'})"}) {
            try (PreparedStatement r = remote.prepareStatement(sql); PreparedStatement l = local.prepareStatement(sql)) {
                assertEquals(l.getParameterMetaData().getParameterCount(), r.getParameterMetaData().getParameterCount(),
                        sql);
            }
        }
        // what the warehouse does not report is refused by name, never guessed
        try (PreparedStatement r = remote.prepareStatement("SELECT ?")) {
            java.sql.ParameterMetaData md = r.getParameterMetaData();
            assertThrows(SQLFeatureNotSupportedException.class, () -> md.getParameterType(1));
            assertThrows(SQLFeatureNotSupportedException.class, () -> md.getParameterTypeName(1));
        }
    }

    @Test
    void askingAnInsertsParameterCountRunsNothingOfIt() throws SQLException {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            try (PreparedStatement p = c.prepareStatement("INSERT INTO counted VALUES (?, ?)")) {
                assertEquals(2, p.getParameterMetaData().getParameterCount());
            }
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM counted")) {
                assertTrue(rs.next());
                assertEquals(0L, rs.getLong(1));
            }
        }
    }

    /** The exec seam's file load's statement (DuckDb.csvStatements), into {@code table}. */
    static final String READ_CSV = " SELECT * FROM read_csv(?, header = true, delim = ',', quote = '\"',"
            + " escape = '\"', auto_detect = false, strict_mode = true, nullstr = '',"
            + " columns = {'c0': 'VARCHAR', 'c1': 'VARCHAR'})";

    @Test
    void aFileIsReadThroughItsBoundPathAsTheExecSeamLoadsOne() throws Exception {
        // JdbcStatements.readsFile: the count asked first, the path then bound as the statement's one value -- a file
        // where the warehouse reads files, its import folder (Database.lockDown: an owner loads data from there)
        Path csv = Files.createDirectories(data.resolve("import")).resolve("loaded.csv");
        Files.writeString(csv, "id,name\n1,a\n2,b\n");
        try (Connection c = connect(); Statement s = c.createStatement()) {
            try (PreparedStatement p = c.prepareStatement("INSERT INTO loaded" + READ_CSV)) {
                assertEquals(1, p.getParameterMetaData().getParameterCount());
                p.setString(1, csv.toString());
                assertFalse(p.execute());
            }
            try (ResultSet rs = s.executeQuery("SELECT string_agg(id || name, ',' ORDER BY id) FROM loaded")) {
                assertTrue(rs.next());
                assertEquals("1a,2b", rs.getString(1));
            }
        } finally {
            Files.deleteIfExists(csv);
        }
    }

    @Test
    void aFileOutsideTheWarehousesImportFolderIsRefusedThereAsItsLockDownSays() throws Exception {
        // the path is bound and reaches the warehouse; the warehouse's DuckDB reads no file outside its import folder
        // (Database.lockDown), so a file the caller staged elsewhere is refused by DuckDB in its own words
        Path csv = Files.createTempFile("warehouse-outside", ".csv");
        Files.writeString(csv, "id,name\n1,a\n");
        try (Connection c = connect()) {
            try (PreparedStatement p = c.prepareStatement(READ_CSV.strip())) {
                p.setString(1, csv.toString());
                SQLException e = assertThrows(SQLException.class, p::executeQuery);
                assertTrue(e.getMessage().contains("file system operations are disabled by configuration"),
                        e.getMessage());
            }
        } finally {
            Files.deleteIfExists(csv);
        }
    }

    @Test
    void aClosedStatementTakesNoValue() throws SQLException {
        PreparedStatement p = remote.prepareStatement("SELECT ?");
        p.close();
        assertThrows(SQLException.class, () -> p.setString(1, "late"));
    }
}
