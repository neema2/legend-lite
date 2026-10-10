import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

/**
 * A DateTime parameter against its literal, to the nanosecond (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3):
 * per value, the literals a dialect writes, then the value bound bare and in each cast, passed as a LocalDateTime, a
 * Timestamp and its text. Run on DuckDB: java -cp duckdb_jdbc-1.4.4.0.jar TimestampProbe.java jdbc:duckdb:
 */
public class TimestampProbe {
    static String run(Connection c, String sql, Object v, String how) {
        try (PreparedStatement st = c.prepareStatement(sql)) {
            if (v != null) {
                switch (how) {
                    case "object" -> st.setObject(1, v);
                    case "timestamp" -> st.setTimestamp(1, Timestamp.valueOf((LocalDateTime) v));
                    case "string" -> st.setString(1, v.toString().replace('T', ' '));
                    default -> throw new IllegalStateException(how);
                }
            }
            try (ResultSet rs = st.executeQuery()) {
                rs.next();
                return rs.getMetaData().getColumnTypeName(1) + " " + rs.getString(1);
            }
        } catch (Exception e) {
            return "FAIL " + String.valueOf(e.getMessage()).split("\n")[0];
        }
    }

    public static void main(String[] a) throws Exception {
        try (Connection c = DriverManager.getConnection(a[0])) {
            System.out.println("== " + c.getMetaData().getDatabaseProductName() + " " + c.getMetaData().getDatabaseProductVersion());
            for (LocalDateTime v : List.of(LocalDateTime.of(2024, 1, 2, 10, 30), LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_000_000),
                    LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_456_000), LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_456_789),
                    LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_999))) {
                String iso = v.toString().replace('T', ' ');
                if (!iso.contains(".")) iso = iso + (iso.length() == 16 ? ":00" : "");
                System.out.println("-- " + v);
                for (String lit : List.of("TIMESTAMP '" + iso + "'", "TIMESTAMP_NS '" + iso + "'")) {
                    System.out.println("  literal " + lit + ": " + run(c, "SELECT CAST(" + lit + " AS VARCHAR)", null, null)
                            + " | json " + run(c, "SELECT CAST(json_object('t', " + lit + ") AS VARCHAR)", null, null));
                }
                for (String cast : List.of("?", "CAST(? AS TIMESTAMP)", "CAST(? AS TIMESTAMP_NS)")) {
                    for (String how : List.of("object", "timestamp", "string")) {
                        System.out.println("  " + cast + " by " + how + ": " + run(c, "SELECT CAST(" + cast + " AS VARCHAR)", v, how)
                                + " | json " + run(c, "SELECT CAST(json_object('t', " + cast + ") AS VARCHAR)", v, how));
                    }
                }
            }
        }
    }
}
