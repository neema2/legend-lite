import java.sql.*;
import java.nio.file.*;
import java.util.*;

/** Usage: H <formula-file> : the formula with {x} and {y}; checks it against Java's % bit for bit. */
public class H {
    public static void main(String[] a) throws Exception {
        String f = Files.readString(Path.of(a[0])).trim();
        List<double[]> pairs = new ArrayList<>();
        double[] specials = {0.0, -0.0, 1.0, -1.0, 0.1, 0.3, 1.7, 7, 3, 1.5, -1.5, 5.5, 1e300, 1e17, 1e-300,
            Double.MIN_VALUE, -Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE, -Double.MAX_VALUE,
            Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 2.5, 1e15, 9007199254740993.0, 4.9e-322, 0.5};
        for (double x : specials) for (double y : specials) pairs.add(new double[]{x, y});
        Random r = new Random(7);
        for (int i = 0; i < 4000; i++) {
            double x = Double.longBitsToDouble(r.nextLong()), y = Double.longBitsToDouble(r.nextLong());
            if (i % 3 == 0) { y = x / (1 + r.nextInt(1000) + r.nextDouble()); }
            if (i % 5 == 0) { x = (r.nextInt(2000000) - 1000000) / 64.0; y = (r.nextInt(1000) + 1) / 8.0; }
            pairs.add(new double[]{x, y});
        }
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:55432/postgres", "pg", "")) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS t");
                s.execute("CREATE TABLE t(i int, x float8, y float8)");
            }
            try (PreparedStatement p = c.prepareStatement("INSERT INTO t VALUES (?, ?, ?)")) {
                for (int i = 0; i < pairs.size(); i++) {
                    p.setInt(1, i); p.setDouble(2, pairs.get(i)[0]); p.setDouble(3, pairs.get(i)[1]); p.addBatch();
                }
                p.executeBatch();
            }
            String sql = "SELECT i, " + f.replace("{x}", "t.x").replace("{y}", "t.y")
                    + " FROM t WHERE t.y <> 0 OR t.y IS NULL ORDER BY i";
            int wrong = 0, n = 0;
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
                while (rs.next()) {
                    n++;
                    int i = rs.getInt(1);
                    double got = rs.getDouble(2);
                    boolean nul = rs.wasNull();
                    double want = pairs.get(i)[0] % pairs.get(i)[1];
                    if (nul || Double.doubleToLongBits(got) != Double.doubleToLongBits(want)) {
                        if (wrong++ < 8) System.out.println("WRONG " + pairs.get(i)[0] + " rem " + pairs.get(i)[1]
                                + ": got " + (nul ? "NULL" : got) + " want " + want);
                    }
                }
            }
            System.out.println("checked " + n + ", wrong " + wrong + ", formula chars per t.x/t.y: " + f.length());
            // the grouped select (the Cypher island's shape): the key is an expression
            String g = "SELECT g, " + f.replace("{x}", "g").replace("{y}", "CAST(2.0 AS float8)")
                    + " FROM (SELECT x * 1.5 AS g FROM t WHERE i < 30) s GROUP BY g";
            String g2 = "SELECT x * 1.5 AS g, count(*), " + f.replace("{x}", "x * 1.5").replace("{y}", "CAST(2.0 AS float8)")
                    + " FROM t WHERE i < 30 GROUP BY x * 1.5";
            try (Statement s = c.createStatement()) {
                s.executeQuery(g2).close();
                System.out.println("grouped select by an expression: runs");
            } catch (SQLException e) {
                System.out.println("grouped select by an expression: FAILS " + e.getMessage());
            }
        }
    }
}
