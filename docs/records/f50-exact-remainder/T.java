import java.sql.*; import java.nio.file.*;
public class T { public static void main(String[] a) throws Exception {
  try (Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:55432/postgres", "pg", ""); Statement s = c.createStatement()) {
    s.execute("SET jit = off"); s.execute("SET max_parallel_workers_per_gather = 0"); s.execute("DROP TABLE IF EXISTS big"); s.execute("CREATE TABLE big AS SELECT random()*1e6 AS x, random()*100 + 0.001 AS y FROM generate_series(1, 1000000)");
    s.execute("ANALYZE big");
    for (int round = 0; round < 2; round++) for (String f : a) {
      String q = "SELECT sum(" + Files.readString(Path.of(f)).trim().replace("{x}", "x").replace("{y}", "y") + ") FROM big";
      long t0 = System.nanoTime(); try (ResultSet r = s.executeQuery(q)) { r.next(); } 
      System.out.println(f + ": " + (System.nanoTime() - t0) / 1_000_000 + " ms");
    }
  } } }
