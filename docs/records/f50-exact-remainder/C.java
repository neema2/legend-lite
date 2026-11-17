import java.sql.*; import java.nio.file.*;
public class C { public static void main(String[] a) throws Exception {
  String f = Files.readString(Path.of(a[0])).trim();
  String[] consts = {"CAST(0 AS DOUBLE PRECISION)", "CAST('-0' AS DOUBLE PRECISION)", "CAST('Infinity' AS DOUBLE PRECISION)",
      "CAST('-Infinity' AS DOUBLE PRECISION)", "CAST('NaN' AS DOUBLE PRECISION)", "CAST(NULL AS DOUBLE PRECISION)",
      "CAST(4.9e-324 AS DOUBLE PRECISION)", "CAST(1e300 AS DOUBLE PRECISION)", "CAST(7 AS DOUBLE PRECISION)"};
  int bad = 0;
  try (Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:55432/postgres", "pg", ""); Statement s = c.createStatement()) {
    for (String k : consts) for (String side : new String[]{"x", "y", "both"}) {
      String x = side.equals("y") ? "t.x" : k, y = side.equals("x") ? "t.y" : k;
      if (side.equals("both")) { x = k; y = "CAST(3 AS DOUBLE PRECISION)"; }
      String q = "SELECT " + f.replace("{x}", x).replace("{y}", y) + " FROM t WHERE i < 50 AND (t.y <> 0 OR " + (side.equals("x") ? "false" : "true") + ")";
      try { s.executeQuery(q).close(); } catch (SQLException e) { bad++; System.out.println("FAILS " + side + "=" + k + ": " + e.getMessage().split("\n")[0]); }
    }
  }
  System.out.println("constant operands: " + bad + " failing");
} }
