import java.sql.*;
import java.util.*;

public class BindProbe {
    static void run(Connection c, String label, String sql, Binder b) {
        try (PreparedStatement st = c.prepareStatement(sql)) {
            b.bind(c, st);
            List<String> out = new ArrayList<>();
            try (ResultSet rs = st.executeQuery()) { while (rs.next()) out.add(rs.getString(1)); }
            System.out.println("OK   " + label + " -> " + out);
        } catch (Exception e) {
            System.out.println("FAIL " + label + " -> " + e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0]);
        }
    }
    interface Binder { void bind(Connection c, PreparedStatement st) throws Exception; }

    public static void main(String[] a) throws Exception {
        String url = a[0];
        try (Connection c = DriverManager.getConnection(url, a.length > 1 ? a[1] : "sa", a.length > 2 ? a[2] : "")) {
            System.out.println("== " + c.getMetaData().getDatabaseProductName() + " " + c.getMetaData().getDriverVersion());
            try (Statement s = c.createStatement()) {
                s.execute("create table T(ID INTEGER, NAME VARCHAR(20))");
                s.execute("insert into T values (1,'a'),(2,'O''Brien'),(3,'c')");
            }
            run(c, "scalar string with a quote", "select ID from T where NAME = ?", (cn, st) -> st.setString(1, "O'Brien"));
            run(c, "list: = ANY(?) createArrayOf", "select ID from T where ID = ANY(?) order by ID",
                    (cn, st) -> st.setArray(1, cn.createArrayOf("INTEGER", new Object[]{1, 3})));
            run(c, "list: = ANY(?) setObject(Integer[])", "select ID from T where ID = ANY(?) order by ID",
                    (cn, st) -> st.setObject(1, new Integer[]{1, 3}));
            run(c, "list: = ANY(?) empty array", "select ID from T where ID = ANY(?) order by ID",
                    (cn, st) -> st.setArray(1, cn.createArrayOf("INTEGER", new Object[]{})));
            run(c, "list of strings: = ANY(?)", "select ID from T where NAME = ANY(?) order by ID",
                    (cn, st) -> st.setArray(1, cn.createArrayOf("VARCHAR", new Object[]{"a", "O'Brien"})));
            run(c, "null scalar: NAME = ? (null)", "select ID from T where NAME = ?", (cn, st) -> st.setNull(1, Types.VARCHAR));
        }
    }
}
