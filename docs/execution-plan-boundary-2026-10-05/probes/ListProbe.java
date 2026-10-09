import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;

/**
 * A list parameter bound as one array (step 2's landing 2, slice (e)): {@code col = ANY(?)} against the literal list a
 * let writes today ({@code col IN (...)}), per element type, with the array made by {@code createArrayOf} under the
 * element type names a plan could carry, bare and (H2's form) cast; and the empty list.
 */
public class ListProbe {
    interface Binder { void bind(Connection c, PreparedStatement st) throws Exception; }

    static String run(Connection c, String sql, Binder b) {
        try (PreparedStatement st = c.prepareStatement(sql)) {
            if (b != null) b.bind(c, st);
            try (ResultSet rs = st.executeQuery()) {
                List<String> out = new ArrayList<>();
                while (rs.next()) out.add(rs.getString(1));
                return out.toString();
            }
        } catch (Exception e) {
            return "FAIL " + e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0];
        }
    }

    record Kind(String column, String literal, String[] typeNames, String h2Cast, Object[] values) {}

    public static void main(String[] a) throws Exception {
        try (Connection c = DriverManager.getConnection(a[0], a.length > 1 ? a[1] : "sa", a.length > 2 ? a[2] : "")) {
            String db = c.getMetaData().getDatabaseProductName();
            System.out.println("== " + db + " " + c.getMetaData().getDatabaseProductVersion());
            try (Statement s = c.createStatement()) {
                s.execute("create table T(ID INTEGER, NAME VARCHAR(20), PRICE DECIMAL(10,2), D DATE, TS TIMESTAMP, B BOOLEAN,"
                        + " P DECIMAL(10,4))");
                s.execute("insert into T values (1, 'a', 1.50, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00', TRUE,"
                        + " 0.1234), (2, 'b', 2.50, DATE '2024-02-01', TIMESTAMP '2024-02-01 10:00:00', FALSE, 0.1230),"
                        + " (3, 'c', 3.25, DATE '2024-03-01', TIMESTAMP '2024-03-01 10:00:00', TRUE, 12345.6789),"
                        + " (4, NULL, NULL, NULL, NULL, NULL, NULL)");
            }
            List<Kind> kinds = List.of(
                    new Kind("ID", "1, 3", new String[]{"BIGINT", "INTEGER", "int8"}, "BIGINT ARRAY", new Object[]{1L, 3L}),
                    new Kind("NAME", "'a', 'c'", new String[]{"VARCHAR", "varchar", "text"}, "VARCHAR ARRAY",
                            new Object[]{"a", "c"}),
                    new Kind("PRICE", "1.50, 3.25", new String[]{"DECIMAL", "NUMERIC", "numeric"}, "DECIMAL(38,2) ARRAY",
                            new Object[]{new BigDecimal("1.50"), new BigDecimal("3.25")}),
                    new Kind("D", "DATE '2024-01-01', DATE '2024-03-01'", new String[]{"DATE", "date"}, "DATE ARRAY",
                            new Object[]{LocalDate.of(2024, 1, 1), LocalDate.of(2024, 3, 1)}),
                    new Kind("TS", "TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-03-01 10:00:00'",
                            new String[]{"TIMESTAMP", "timestamp"}, "TIMESTAMP ARRAY",
                            new Object[]{java.sql.Timestamp.valueOf("2024-01-01 10:00:00"),
                                    java.sql.Timestamp.valueOf("2024-03-01 10:00:00")}),
                    new Kind("B", "TRUE", new String[]{"BOOLEAN", "boolean"}, "BOOLEAN ARRAY", new Object[]{true}),
                    // a decimal of more than three places, and one of nine digits: does the array keep them?
                    new Kind("P", "0.1234, 12345.6789", new String[]{"DECIMAL", "NUMERIC"}, "DECIMAL(38,4) ARRAY",
                            new Object[]{new BigDecimal("0.1234"), new BigDecimal("12345.6789")}));
            for (Kind k : kinds) {
                System.out.println("-- " + k.column());
                System.out.println("  literal IN: " + run(c, "select ID from T where " + k.column() + " in (" + k.literal()
                        + ") order by ID", null));
                for (String t : k.typeNames()) {
                    Binder b = (cn, st) -> st.setArray(1, cn.createArrayOf(t, k.values()));
                    Binder empty = (cn, st) -> st.setArray(1, cn.createArrayOf(t, new Object[]{}));
                    System.out.println("  = ANY(?) " + t + ": " + run(c, "select ID from T where " + k.column()
                            + " = ANY(?) order by ID", b) + "; empty " + run(c, "select ID from T where " + k.column()
                            + " = ANY(?) order by ID", empty));
                    if (db.equals("H2")) {
                        System.out.println("  = ANY(CAST(? AS " + k.h2Cast() + ")) " + t + ": " + run(c,
                                "select ID from T where " + k.column() + " = ANY(CAST(? AS " + k.h2Cast() + ")) order by ID",
                                b) + "; empty " + run(c, "select ID from T where " + k.column() + " = ANY(CAST(? AS "
                                + k.h2Cast() + ")) order by ID", empty));
                    }
                }
            }
        }
    }
}
