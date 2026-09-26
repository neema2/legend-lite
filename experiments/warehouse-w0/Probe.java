import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/** W0 probe: the warehouse wrapper's skeleton -- DuckDB behind an HTTP SQL call. */
public final class Probe {
  static long t0 = System.nanoTime();

  static String run(Connection db, String sql, String user) throws Exception {
    try (Statement st = db.createStatement()) {
      // The identity, set before every statement, on this statement's own connection.
      st.execute("SET VARIABLE app_user = '" + user.replace("'", "''") + "'");
      StringBuilder out = new StringBuilder("{\"columns\":[");
      try (ResultSet rs = st.executeQuery(sql)) {
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
          if (i > 1) out.append(',');
          out.append('"').append(md.getColumnName(i)).append('"');
        }
        out.append("],\"rows\":[");
        boolean first = true;
        while (rs.next()) {
          if (!first) out.append(',');
          first = false;
          out.append('[');
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (i > 1) out.append(',');
            Object v = rs.getObject(i);
            if (v == null) out.append("null");
            else if (v instanceof Number) out.append(v);
            else out.append('"').append(v.toString().replace("\"", "\\\"")).append('"');
          }
          out.append(']');
        }
      }
      return out.append("]}").toString();
    }
  }

  public static void main(String[] args) throws Exception {
    String dbPath = System.getenv().getOrDefault("DB", "");
    Connection root = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
    long tDb = System.nanoTime();
    if (args.length > 0 && args[0].equals("--once")) {
      try (Connection c = ((org.duckdb.DuckDBConnection) root).duplicate()) {
        String result = run(c, args[1], "user1");
        long tDone = System.nanoTime();
        System.out.println(result);
        System.err.printf("open-db %.1f ms, first-result %.1f ms (in-process, from JVM/image entry)%n",
            (tDb - t0) / 1e6, (tDone - t0) / 1e6);
      }
      return;
    }
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", Integer.parseInt(args.length > 0 ? args[0] : "8765")), 0);
    server.createContext("/sql/v1/statements", ex -> {
      byte[] body;
      int status = 200;
      try (Connection c = ((org.duckdb.DuckDBConnection) root).duplicate()) {
        String sql = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String user = ex.getRequestHeaders().getFirst("X-Probe-User");
        body = run(c, sql, user == null ? "anonymous" : user).getBytes(StandardCharsets.UTF_8);
      } catch (Exception e) {
        status = 400;
        body = ("{\"error\":\"" + String.valueOf(e.getMessage()).replace("\"", "'") + "\"}").getBytes(StandardCharsets.UTF_8);
      }
      ex.getResponseHeaders().set("Content-Type", "application/json");
      ex.sendResponseHeaders(status, body.length);
      try (OutputStream os = ex.getResponseBody()) { os.write(body); }
    });
    server.start();
    System.err.printf("listening; open-db %.1f ms, ready %.1f ms%n", (tDb - t0) / 1e6, (System.nanoTime() - t0) / 1e6);
  }
}
