import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import org.duckdb.*;

/**
 * W1d homework: Arrow result chunks, measured (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md §5).
 * Outside the Bazel build; run with DuckDB JDBC 1.5.5.1 on the classpath:
 *   javac -cp duckdb.jar ArrowProbe.java && java -cp .:duckdb.jar ArrowProbe <outdir>
 * (a) nanoarrow's to_arrow_ipc, (b) row-by-row getObject, (c) the 1.5 driver's columnar chunks;
 * then one Arrow stream per API type written to <outdir> for pyarrow to read (arrow_check.py).
 */
public class ArrowProbe {
    static final String BIG = """
        SELECT i::INTEGER a, i::BIGINT b, i * 1.5::DOUBLE c, (i / 7)::DECIMAL(18,3) d,
               'row ' || i e, DATE '2020-01-01' + (i % 1000)::INTEGER f,
               TIMESTAMP '2020-01-01' + to_seconds(i) g, i % 2 = 0 h
        FROM range(1000000) t(i)""";

    public static void main(String[] a) throws Exception {
        Path out = Path.of(a[0]);
        Files.createDirectories(out);
        try (DuckDBConnection c = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
             Statement st = c.createStatement()) {
            st.execute("SET threads=4");
            long t0 = System.nanoTime();
            st.execute("INSTALL nanoarrow FROM community");
            st.execute("LOAD nanoarrow");
            System.out.printf("nanoarrow install+load: %d ms%n", (System.nanoTime() - t0) / 1_000_000);
            st.execute("CREATE TABLE big AS " + BIG);
            for (int rep = 0; rep < 3; rep++) {
                System.out.println("-- rep " + rep);
                nanoarrow(c, "SELECT * FROM big", out.resolve("big.arrows"));
                rows(c, "SELECT * FROM big");
                chunks(c, "SELECT * FROM big");
            }
        }
        coverage(out);
    }

    static void nanoarrow(Connection c, String q, Path file) throws Exception {
        long t0 = System.nanoTime();
        long bytes = 0, blobs = 0;
        try (PreparedStatement p = c.prepareStatement("SELECT * FROM to_arrow_ipc((" + q + "))");
             ResultSet rs = p.executeQuery();
             OutputStream os = new BufferedOutputStream(Files.newOutputStream(file))) {
            while (rs.next()) {
                byte[] b = rs.getBytes(1);
                bytes += b.length;
                blobs++;
                os.write(b);
            }
        }
        System.out.printf("(a) nanoarrow to_arrow_ipc: %d ms, %d blobs, %.1f MB%n",
                (System.nanoTime() - t0) / 1_000_000, blobs, bytes / 1e6);
    }

    static void rows(Connection c, String q) throws Exception {
        long t0 = System.nanoTime();
        long n = 0, h = 0;
        try (PreparedStatement p = c.prepareStatement(q); ResultSet rs = p.executeQuery()) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    Object o = rs.getObject(i);
                    h += o == null ? 0 : 1;
                }
                n++;
            }
        }
        System.out.printf("(b) row-by-row getObject: %d ms (%d rows, %d cells)%n",
                (System.nanoTime() - t0) / 1_000_000, n, h);
    }

    static void chunks(Connection c, String q) throws Exception {
        long t0 = System.nanoTime();
        long n = 0, h = 0;
        try (DuckDBPreparedStatement p = c.prepareStatement(q).unwrap(DuckDBPreparedStatement.class);
             DuckDBChunkedResult r = p.query()) {
            int cols = (int) r.columnCount();
            while (r.nextChunk()) {
                DuckDBDataChunkReader ch = r.chunk();
                long rows = ch.rowCount();
                for (int col = 0; col < cols; col++) {
                    DuckDBReadableVector v = ch.vector(col);
                    for (long i = 0; i < rows; i++) {
                        if (v.isNull(i)) continue;
                        switch (col) {
                            case 0 -> h += v.getInt(i);
                            case 1 -> h += v.getLong(i);
                            case 2 -> h += (long) v.getDouble(i);
                            case 3 -> h += v.getBigDecimal(i).signum();
                            case 4 -> h += v.getString(i).length();
                            case 5 -> h += v.getLocalDate(i).getDayOfMonth();
                            case 6 -> h += v.getLocalDateTime(i).getSecond();
                            default -> h += v.getBoolean(i) ? 1 : 0;
                        }
                    }
                }
                n += rows;
            }
        }
        System.out.printf("(c) columnar chunks, typed getters: %d ms (%d rows, h=%d)%n",
                (System.nanoTime() - t0) / 1_000_000, n, h);
    }

    /** One small stream per API type, for arrow_check.py; a type nanoarrow refuses is printed.
     *  A FRESH database per type: an INTERNAL error invalidates the database it happens in. */
    static void coverage(Path out) throws Exception {
        String[][] types = {
            {"boolean", "true"}, {"tinyint", "1::TINYINT"}, {"smallint", "1::SMALLINT"}, {"integer", "1::INTEGER"},
            {"bigint", "9007199254740993::BIGINT"}, {"hugeint", "170141183460469231731687303715884105727::HUGEINT"},
            {"utinyint", "255::UTINYINT"}, {"usmallint", "65535::USMALLINT"}, {"uinteger", "4294967295::UINTEGER"},
            {"ubigint", "18446744073709551615::UBIGINT"}, {"uhugeint", "1::UHUGEINT"},
            {"float", "1.5::FLOAT"}, {"double", "'-0.0'::DOUBLE"}, {"decimal", "123.45::DECIMAL(9,2)"},
            {"decimal38", "1.5::DECIMAL(38,10)"}, {"varchar", "'héllo'"}, {"uuid", "'11111111-2222-3333-4444-555555555555'::UUID"},
            {"interval", "INTERVAL 1 DAY + INTERVAL 2 HOUR"}, {"enum", "'a'::ENUM('a','b')"}, {"json", "'{\"a\":1}'::JSON"},
            {"blob", "'\\xAA\\xBB'::BLOB"}, {"date", "DATE '0001-01-01'"}, {"time", "TIME '01:02:03.456789'"},
            {"timestamp", "TIMESTAMP '1500-01-01 01:02:03.123456'"}, {"timestamp_s", "TIMESTAMP_S '2020-01-01 01:02:03'"},
            {"timestamp_ms", "TIMESTAMP_MS '2020-01-01 01:02:03.123'"}, {"timestamp_ns", "TIMESTAMP_NS '2020-01-01 01:02:03.123456789'"},
            {"timestamptz", "TIMESTAMPTZ '2020-01-01 01:02:03+05'"}, {"list", "[1, NULL, 3]"}, {"array", "[1, 2]::INTEGER[2]"},
            {"struct", "{'x': 1, 'y': 'b'}"}, {"map", "MAP {'k': 1}"}, {"nested", "[{'x': [1.5::DECIMAL(4,1)]}]"},
        };
        for (String[] t : types) {
            String q = "SELECT " + t[1] + " AS v UNION ALL SELECT NULL";
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
                st.execute("LOAD nanoarrow");
                try {
                    nanoarrowQuiet(c, q, out.resolve(t[0] + ".arrows"));
                } catch (SQLException e) {
                    String alive;
                    try (ResultSet rs = st.executeQuery("SELECT 1")) {
                        alive = "database still usable";
                    } catch (SQLException dead) {
                        alive = "DATABASE INVALIDATED";
                    }
                    System.out.println("nanoarrow REFUSED " + t[0] + " (" + alive + "): "
                            + e.getMessage().lines().findFirst().orElse(""));
                }
            }
        }
    }

    static void nanoarrowQuiet(Connection c, String q, Path file) throws Exception {
        try (PreparedStatement p = c.prepareStatement("SELECT * FROM to_arrow_ipc((" + q + "))");
             ResultSet rs = p.executeQuery();
             OutputStream os = Files.newOutputStream(file)) {
            while (rs.next()) os.write(rs.getBytes(1));
        }
    }
}
