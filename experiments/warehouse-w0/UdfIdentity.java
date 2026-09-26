import java.sql.*;
import org.duckdb.*;

public final class UdfIdentity {
  static final ThreadLocal<String> PRINCIPAL = new ThreadLocal<>();
  static final java.util.Set<String> THREADS = java.util.concurrent.ConcurrentHashMap.newKeySet();

  public static void main(String[] a) throws Exception {
    Connection root = DriverManager.getConnection("jdbc:duckdb:");
    Connection c1 = ((DuckDBConnection) root).duplicate();
    Connection c2 = ((DuckDBConnection) root).duplicate();

    // Q1: is a function registered through c1 visible from c2?
    DuckDBFunctions.scalarFunction().withName("only_c1").withReturnType(String.class)
        .withFunction((java.util.function.Supplier<String>) () -> "from-c1").register(c1);
    System.out.println("c1 sees only_c1: " + one(c1, "SELECT only_c1()"));
    try { System.out.println("c2 sees only_c1: " + one(c2, "SELECT only_c1()")); }
    catch (SQLException e) { System.out.println("c2 cannot see only_c1: " + e.getMessage().split("\n")[0]); }
    try {
      DuckDBFunctions.scalarFunction().withName("only_c1").withReturnType(String.class)
          .withFunction((java.util.function.Supplier<String>) () -> "from-c2").register(c2);
      System.out.println("c2 re-registered only_c1 -> c1 now sees: " + one(c1, "SELECT only_c1()"));
    } catch (Exception e) { System.out.println("c2 cannot re-register: " + e.getMessage().split("\n")[0]); }

    // Q2: a principal read from a thread-local the server sets before each statement.
    DuckDBFunctions.scalarFunction().withName("principal").withReturnType(String.class).withVolatile()
        .withFunction((java.util.function.Supplier<String>) () -> {
          THREADS.add(Thread.currentThread().getName());
          String p = PRINCIPAL.get();
          if (p == null) throw new IllegalStateException("no principal on thread " + Thread.currentThread().getName());
          return p;
        }).register(root);
    try (Statement s = c1.createStatement()) {
      s.execute("CREATE TABLE big AS SELECT i, i % 7 AS k FROM range(10000000) t(i)");
    }
    PRINCIPAL.set("user1");
    System.out.println("small query: " + one(c1, "SELECT principal()"));
    THREADS.clear();
    try {
      System.out.println("large parallel query: " + one(c1,
          "SELECT count(*) FROM big WHERE principal() = 'user1' AND k = 3"));
    } catch (SQLException e) { System.out.println("large parallel query FAILED: " + e.getMessage().split("\n")[0]); }
    System.out.println("threads that ran principal(): " + THREADS);
  }

  static String one(Connection c, String sql) throws SQLException {
    try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) { r.next(); return r.getString(1); }
  }
}
