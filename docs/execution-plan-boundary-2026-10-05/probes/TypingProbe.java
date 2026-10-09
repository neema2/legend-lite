import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/** Where a bare {@code ?} is typed by the database and where it is not: one value bound by JDBC, per position. */
public class TypingProbe {
    static void run(Connection c, String label, String sql, Binder b) {
        try (PreparedStatement st = c.prepareStatement(sql)) {
            b.bind(c, st);
            List<String> out = new ArrayList<>();
            try (ResultSet rs = st.executeQuery()) { while (rs.next()) out.add(rs.getString(1)); }
            System.out.println("OK   " + label + " -> " + out);
        } catch (Exception e) {
            System.out.println("FAIL " + label + " -> " + e.getClass().getSimpleName() + ": "
                    + String.valueOf(e.getMessage()).split("\n")[0]);
        }
    }
    interface Binder { void bind(Connection c, PreparedStatement st) throws Exception; }

    public static void main(String[] a) throws Exception {
        String url = a[0];
        try (Connection c = DriverManager.getConnection(url, a.length > 1 ? a[1] : "sa", a.length > 2 ? a[2] : "")) {
            System.out.println("== " + c.getMetaData().getDatabaseProductName() + " "
                    + c.getMetaData().getDatabaseProductVersion() + " / driver " + c.getMetaData().getDriverVersion());
            try (Statement s = c.createStatement()) {
                s.execute("create table T(ID INTEGER, NAME VARCHAR(20), PRICE DECIMAL(10,2), D DATE, TS TIMESTAMP)");
                s.execute("insert into T values (1,'a',1.50,DATE '2024-01-01',TIMESTAMP '2024-01-01 10:00:00'),"
                        + "(2,'b',2.50,DATE '2024-02-01',TIMESTAMP '2024-02-01 10:00:00'),"
                        + "(3,NULL,3.50,DATE '2024-03-01',TIMESTAMP '2024-03-01 10:00:00')");
            }
            // compared with a column: the column types the placeholder
            run(c, "column = ? (Long)", "select ID from T where ID = ? order by ID", (cn, st) -> st.setLong(1, 2));
            run(c, "? = column (Long)", "select ID from T where ? = ID order by ID", (cn, st) -> st.setLong(1, 2));
            run(c, "column > ? (BigDecimal)", "select ID from T where PRICE > ? order by ID",
                    (cn, st) -> st.setBigDecimal(1, new BigDecimal("2.00")));
            run(c, "column = ? (LocalDate)", "select ID from T where D = ? order by ID",
                    (cn, st) -> st.setObject(1, LocalDate.of(2024, 2, 1)));
            run(c, "column > ? (LocalDateTime)", "select ID from T where TS > ? order by ID",
                    (cn, st) -> st.setObject(1, LocalDateTime.of(2024, 1, 15, 0, 0)));
            run(c, "column LIKE ? (String)", "select ID from T where NAME LIKE ? order by ID",
                    (cn, st) -> st.setString(1, "a%"));
            // arithmetic and functions over a column
            run(c, "column + ? (Long)", "select ID + ? from T order by ID", (cn, st) -> st.setLong(1, 10));
            run(c, "column * ? (Double)", "select PRICE * ? from T order by ID", (cn, st) -> st.setDouble(1, 2.0));
            run(c, "column || ? (String)", "select NAME || ? from T order by ID", (cn, st) -> st.setString(1, "!"));
            run(c, "coalesce(column, ?) (String)", "select coalesce(NAME, ?) from T order by ID",
                    (cn, st) -> st.setString(1, "none"));
            // the placeholder alone: nothing types it but the bound value
            run(c, "projection: ? (String)", "select ? from T order by ID", (cn, st) -> st.setString(1, "x"));
            run(c, "projection: ? (Long)", "select ? from T order by ID", (cn, st) -> st.setLong(1, 7));
            run(c, "? + 1 (Long)", "select ? + 1 from T order by ID", (cn, st) -> st.setLong(1, 7));
            run(c, "upper(?) (String)", "select upper(?) from T order by ID", (cn, st) -> st.setString(1, "x"));
            run(c, "? > 1 (Long) in WHERE", "select ID from T where ? > 1 order by ID", (cn, st) -> st.setLong(1, 7));
            run(c, "case when ? (Boolean)", "select case when ? then 1 else 0 end from T order by ID",
                    (cn, st) -> st.setBoolean(1, true));
            // an optional value's absence
            run(c, "? IS NULL (null VARCHAR)", "select ID from T where ? IS NULL order by ID",
                    (cn, st) -> st.setNull(1, Types.VARCHAR));
            run(c, "? IS NULL (value)", "select ID from T where ? IS NULL order by ID",
                    (cn, st) -> st.setString(1, "a"));
            run(c, "column IS NOT DISTINCT FROM ? (null)", "select ID from T where NAME IS NOT DISTINCT FROM ? order by ID",
                    (cn, st) -> st.setNull(1, Types.VARCHAR));
            run(c, "column IS NOT DISTINCT FROM ? (value)",
                    "select ID from T where NAME IS NOT DISTINCT FROM ? order by ID", (cn, st) -> st.setString(1, "a"));
            // the same positions with the placeholder typed by a CAST
            run(c, "projection: CAST(? AS VARCHAR)", "select CAST(? AS VARCHAR) from T order by ID",
                    (cn, st) -> st.setString(1, "x"));
            run(c, "CAST(? AS BIGINT) + 1", "select CAST(? AS BIGINT) + 1 from T order by ID",
                    (cn, st) -> st.setLong(1, 7));
            run(c, "upper(CAST(? AS VARCHAR))", "select upper(CAST(? AS VARCHAR)) from T order by ID",
                    (cn, st) -> st.setString(1, "x"));
            run(c, "CAST(? AS VARCHAR) IS NULL (null)", "select ID from T where CAST(? AS VARCHAR) IS NULL order by ID",
                    (cn, st) -> st.setNull(1, Types.VARCHAR));
            run(c, "column = CAST(? AS BIGINT)", "select ID from T where ID = CAST(? AS BIGINT) order by ID",
                    (cn, st) -> st.setLong(1, 2));
            run(c, "column = CAST(? AS DATE) (LocalDate)", "select ID from T where D = CAST(? AS DATE) order by ID",
                    (cn, st) -> st.setObject(1, LocalDate.of(2024, 2, 1)));
        }
    }
}
