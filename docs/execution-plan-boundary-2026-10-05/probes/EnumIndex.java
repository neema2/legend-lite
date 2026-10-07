import java.sql.*;
import java.util.*;

/** Which enum-parameter SQL forms let the database use an index on the stored column? */
public class EnumIndex {
    // ACTIVE is stored as 'A' or 'X' (a two-code mapping); CLOSED as 'C'. 'A'/'X' are rare, 'C' is common.
    static final String TABLE = "(VALUES ('A','ACTIVE'), ('X','ACTIVE'), ('C','CLOSED')) AS M(CODE, NAME)";
    static final Map<String, String> FORMS = new LinkedHashMap<>();
    static {
        FORMS.put("F0 runner translates (control)     ", "select ID from PERSON where STATUS = ANY(?)");
        FORMS.put("F1 value table, IN (select ...)    ", "select ID from PERSON where STATUS in (select CODE from " + TABLE + " where NAME = ?)");
        FORMS.put("F2 value CASE -> one code          ", "select ID from PERSON where STATUS = (case ? when 'ACTIVE' then 'A' when 'CLOSED' then 'C' end)");
        FORMS.put("F3 value CASE -> code array, ANY   ", "select ID from PERSON where STATUS = ANY(case ? when 'ACTIVE' then ARRAY['A','X'] when 'CLOSED' then ARRAY['C'] end)");
        FORMS.put("F4 column decoded (no index expect)", "select ID from PERSON where (case STATUS when 'A' then 'ACTIVE' when 'X' then 'ACTIVE' when 'C' then 'CLOSED' end) = ?");
    }

    public static void main(String[] a) throws Exception {
        try (Connection c = DriverManager.getConnection(a[0], a[1], a[2])) {
            String product = c.getMetaData().getDatabaseProductName();
            System.out.println("== " + product + " " + c.getMetaData().getDatabaseProductVersion());
            try (Statement s = c.createStatement()) {
                s.execute("create table PERSON(ID INTEGER, STATUS VARCHAR(10))");
                if (product.startsWith("PostgreSQL")) {
                    s.execute("insert into PERSON select g, case when g % 2000 = 0 then 'A' when g % 2000 = 1 then 'X' else 'C' end from generate_series(1, 400000) g");
                } else if (product.startsWith("H2")) {
                    s.execute("insert into PERSON select X, case when mod(X, 2000) = 0 then 'A' when mod(X, 2000) = 1 then 'X' else 'C' end from system_range(1, 400000)");
                } else {
                    s.execute("insert into PERSON select g, case when g % 2000 = 0 then 'A' when g % 2000 = 1 then 'X' else 'C' end from range(1, 400001) t(g)");
                }
                s.execute("create index IDX_STATUS on PERSON(STATUS)");
                if (product.startsWith("PostgreSQL")) s.execute("analyze PERSON");
            }
            for (var f : FORMS.entrySet()) {
                String sql = f.getValue();
                try {
                    String plan = explain(c, product, sql);
                    long rows = 0, best = Long.MAX_VALUE;
                    for (int i = 0; i < 15; i++) {
                        long t0 = System.nanoTime();
                        rows = run(c, sql);
                        best = Math.min(best, System.nanoTime() - t0);
                    }
                    System.out.printf("%s  rows=%d  best=%.2fms  index=%s%n", f.getKey(), rows, best / 1e6, usesIndex(product, plan));
                    if (System.getenv("PLANS") != null) System.out.println(plan.replaceAll("(?m)^", "      | "));
                } catch (SQLException e) {
                    System.out.println(f.getKey() + "  FAIL " + String.valueOf(e.getMessage()).split("\n")[0]);
                }
            }
        }
    }

    static void bind(Connection c, PreparedStatement st, String sql) throws SQLException {
        if (sql.contains("STATUS = ANY(?)")) st.setArray(1, c.createArrayOf("VARCHAR", new Object[]{"A", "X"}));
        else st.setString(1, "ACTIVE");
    }

    static long run(Connection c, String sql) throws SQLException {
        try (PreparedStatement st = c.prepareStatement(sql)) {
            bind(c, st, sql);
            long n = 0;
            try (ResultSet rs = st.executeQuery()) { while (rs.next()) n++; }
            return n;
        }
    }

    static String explain(Connection c, String product, String sql) throws SQLException {
        String prefix = product.startsWith("DuckDB") ? "EXPLAIN ANALYZE " : product.startsWith("H2") ? "EXPLAIN " : "EXPLAIN ";
        try (PreparedStatement st = c.prepareStatement(prefix + sql)) {
            bind(c, st, sql);
            StringBuilder out = new StringBuilder();
            try (ResultSet rs = st.executeQuery()) {
                int n = rs.getMetaData().getColumnCount();
                while (rs.next()) out.append(rs.getString(n)).append('\n');
            }
            return out.toString();
        }
    }

    static String usesIndex(String product, String plan) {
        if (product.startsWith("PostgreSQL")) return plan.contains("Index Scan") || plan.contains("Bitmap Index Scan") || plan.contains("Index Only Scan") ? "YES" : "no (" + (plan.contains("Seq Scan") ? "Seq Scan" : "?") + ")";
        if (product.startsWith("H2")) return plan.contains("IDX_STATUS") ? "YES" : "no (table scan)";
        return plan.contains("Index Scan") ? "YES" : "no (" + (plan.contains("SEQ_SCAN") ? "SEQ_SCAN" : plan.contains("TABLE_SCAN") ? "TABLE_SCAN" : "?") + ")";
    }
}
