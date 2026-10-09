import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * A bound value against the literal a let writes today (step 2's landing 2, slice (b)): for each Pure primitive, in the
 * positions a plan's statement puts a parameter -- in arithmetic with a column (a number) or compared with one (the
 * others), and projected in a subquery and consumed outside (the wire's cell formatting) -- whether the statement runs,
 * and the result's type and text: for the literal, the value bound bare, and the value bound in a cast.
 */
public class LiteralProbe {
    interface Binder { void bind(PreparedStatement st, int i) throws Exception; }

    record Kind(String pure, String column, String literal, String cast, Binder bind) {}

    static String run(Connection c, String sql, Binder b) {
        try (PreparedStatement st = c.prepareStatement(sql)) {
            int marks = (int) sql.chars().filter(ch -> ch == '?').count();
            for (int i = 1; i <= marks; i++) b.bind(st, i);
            try (ResultSet rs = st.executeQuery()) {
                List<String> out = new ArrayList<>();
                String type = rs.getMetaData().getColumnTypeName(1) + "(" + rs.getMetaData().getPrecision(1) + ","
                        + rs.getMetaData().getScale(1) + ")";
                while (rs.next()) out.add(rs.getString(1));
                return type + " " + out;
            }
        } catch (Exception e) {
            return "FAIL " + e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0];
        }
    }

    public static void main(String[] a) throws Exception {
        try (Connection c = DriverManager.getConnection(a[0], a.length > 1 ? a[1] : "sa", a.length > 2 ? a[2] : "")) {
            String db = c.getMetaData().getDatabaseProductName();
            System.out.println("== " + db + " " + c.getMetaData().getDatabaseProductVersion());
            try (Statement s = c.createStatement()) {
                s.execute("create table T(ID INTEGER, PRICE DECIMAL(10,2), NAME VARCHAR(20), D DATE, TS TIMESTAMP)");
                s.execute("insert into T values (1, 1.50, 'a', DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00'),"
                        + " (3, 3.25, 'b', DATE '2024-03-01', TIMESTAMP '2024-03-01 10:00:00')");
            }
            String text = db.equals("PostgreSQL") ? "text" : "varchar";
            List<Kind> kinds = List.of(
                    new Kind("Integer 7", "ID", "7", "BIGINT", (st, i) -> st.setLong(i, 7)),
                    new Kind("Float 1.1 as a double", "PRICE", "1.1", "DOUBLE PRECISION", (st, i) -> st.setDouble(i, 1.1)),
                    new Kind("Float 1.1 as a decimal", "PRICE", "1.1", "DECIMAL(38,1)",
                            (st, i) -> st.setBigDecimal(i, new BigDecimal("1.1"))),
                    new Kind("Decimal 2.50", "PRICE", "2.50", "DECIMAL(38,2)",
                            (st, i) -> st.setBigDecimal(i, new BigDecimal("2.50"))),
                    new Kind("String x", "NAME", "'x'", "VARCHAR", (st, i) -> st.setString(i, "x")),
                    new Kind("Boolean true", "(ID > 0)", "TRUE", "BOOLEAN", (st, i) -> st.setBoolean(i, true)),
                    new Kind("StrictDate", "D", "DATE '2024-01-02'", "DATE",
                            (st, i) -> st.setObject(i, LocalDate.of(2024, 1, 2))),
                    new Kind("DateTime", "TS", "TIMESTAMP '2024-01-02 10:30:00'", "TIMESTAMP",
                            (st, i) -> st.setObject(i, LocalDateTime.of(2024, 1, 2, 10, 30))));
            for (Kind k : kinds) {
                System.out.println("-- " + k.pure());
                boolean number = k.pure().startsWith("Integer") || k.pure().startsWith("Float")
                        || k.pure().startsWith("Decimal");
                String with = number ? "select " + k.column() + " * %s from T order by ID"
                        : "select ID from T where " + k.column() + " = %s or " + k.column() + " <> %s order by ID";
                String nested = "select cast(x as " + text + ") from (select %s as x from T) q";
                for (String[] form : List.of(new String[]{"with a column", with}, new String[]{"nested", nested})) {
                    System.out.println("  " + form[0] + ", literal: " + run(c, form[1].replace("%s", k.literal()), k.bind()));
                    System.out.println("  " + form[0] + ", bare:    " + run(c, form[1].replace("%s", "?"), k.bind()));
                    System.out.println("  " + form[0] + ", cast:    "
                            + run(c, form[1].replace("%s", "CAST(? AS " + k.cast() + ")"), k.bind()));
                }
            }
            // a bare ? beside a value of ANOTHER type: does the database type it by its neighbour?
            System.out.println("-- a decimal beside an integer");
            Binder d25 = (st, i) -> st.setBigDecimal(i, new BigDecimal("2.5"));
            Binder d15 = (st, i) -> st.setBigDecimal(i, new BigDecimal("1.5"));
            for (String[] k : new String[][]{{"ID > %s", "2.5"}, {"ID * %s", "1.5"}, {"%s + 1", "2.5"}}) {
                String sql = k[0].startsWith("ID >") ? "select ID from T where " + k[0] + " order by ID"
                        : "select " + k[0] + " from T order by ID";
                Binder b = k[1].equals("2.5") ? d25 : d15;
                System.out.println("  " + k[0] + " " + k[1] + ", literal: " + run(c, sql.replace("%s", k[1]), b));
                System.out.println("  " + k[0] + " " + k[1] + ", bare:    " + run(c, sql.replace("%s", "?"), b));
                System.out.println("  " + k[0] + " " + k[1] + ", DECFLOAT: "
                        + run(c, sql.replace("%s", "CAST(? AS " + (db.equals("H2") ? "DECFLOAT" : "DECIMAL(38,1)") + ")"), b));
            }
            // H2's casts of a decimal: none keeps the value's own scale
            if (db.equals("H2")) {
                System.out.println("-- H2: a decimal cast, alone and times a DECIMAL(10,2) column");
                for (String v : new String[]{"2.50", "1.1", "0.125"}) {
                    Binder b = (st, i) -> st.setBigDecimal(i, new BigDecimal(v));
                    for (String t : new String[]{"NUMERIC", "DECFLOAT", "NUMERIC(38,2)", "DOUBLE PRECISION"}) {
                        System.out.println("  " + v + " as " + t + ": alone " + run(c,
                                "select cast(x as varchar) from (select CAST(? AS " + t + ") as x from T) q", b)
                                + "; times " + run(c, "select PRICE * CAST(? AS " + t + ") from T order by ID", b));
                    }
                    System.out.println("  " + v + " literal: alone " + run(c,
                            "select cast(x as varchar) from (select " + v + " as x from T) q", b)
                            + "; times " + run(c, "select PRICE * " + v + " from T order by ID", b));
                }
            }
        }
    }
}
