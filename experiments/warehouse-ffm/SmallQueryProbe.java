import static java.lang.foreign.ValueLayout.*;
import java.lang.foreign.*; import java.lang.invoke.MethodHandle; import java.nio.file.Path; import java.sql.*;

/** Embedded-mode question: per-statement latency of a small query, DuckDB JDBC vs DuckDB's C API through FFM. */
public class SmallQueryProbe {
  static final StructLayout RESULT = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS);
  static final String Q = "SELECT x, 'v' || x AS s FROM range(10) t(x) WHERE x % 2 = 0";
  public static void main(String[] a) throws Throwable {
    int n = 20_000;
    if (a.length < 2) try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
      for (int rep = 0; rep < 3; rep++) {
        long t0 = System.nanoTime(), h = 0;
        for (int i = 0; i < n; i++) try (PreparedStatement p = c.prepareStatement(Q); ResultSet rs = p.executeQuery()) { while (rs.next()) h += rs.getLong(1) + rs.getString(2).length(); }
        System.out.printf("JDBC: %.1f us per small query (h=%d)%n", (System.nanoTime() - t0) / 1e3 / n, h);
      }
    }
    Linker L = Linker.nativeLinker();
    SymbolLookup lib = SymbolLookup.libraryLookup(Path.of(a[0]), Arena.global());
    MethodHandle open = L.downcallHandle(lib.find("duckdb_open").orElseThrow(), FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
    MethodHandle connect = L.downcallHandle(lib.find("duckdb_connect").orElseThrow(), FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
    MethodHandle query = L.downcallHandle(lib.find("duckdb_query").orElseThrow(), FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
    MethodHandle fetch = L.downcallHandle(lib.find("duckdb_fetch_chunk").orElseThrow(), FunctionDescriptor.of(ADDRESS, RESULT));
    MethodHandle size = L.downcallHandle(lib.find("duckdb_data_chunk_get_size").orElseThrow(), FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    MethodHandle vec = L.downcallHandle(lib.find("duckdb_data_chunk_get_vector").orElseThrow(), FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
    MethodHandle data = L.downcallHandle(lib.find("duckdb_vector_get_data").orElseThrow(), FunctionDescriptor.of(ADDRESS, ADDRESS));
    MethodHandle destroyChunk = L.downcallHandle(lib.find("duckdb_destroy_data_chunk").orElseThrow(), FunctionDescriptor.ofVoid(ADDRESS));
    MethodHandle destroyResult = L.downcallHandle(lib.find("duckdb_destroy_result").orElseThrow(), FunctionDescriptor.ofVoid(ADDRESS));
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment db = arena.allocate(ADDRESS), conn = arena.allocate(ADDRESS);
      int o1 = (int) open.invokeExact(MemorySegment.NULL, db); int o2 = (int) connect.invokeExact(db.get(ADDRESS, 0), conn);
      MemorySegment cn = conn.get(ADDRESS, 0), sql = arena.allocateFrom(Q), res = arena.allocate(RESULT), cp = arena.allocate(ADDRESS);
      for (int rep = 0; rep < 3; rep++) {
        long t0 = System.nanoTime(), h = 0;
        for (int i = 0; i < n; i++) {
          if ((int) query.invokeExact(cn, sql, res) != 0) throw new IllegalStateException();
          MemorySegment ch;
          while (!(ch = (MemorySegment) fetch.invokeExact(res)).equals(MemorySegment.NULL)) {
            long rows = (long) size.invokeExact(ch);
            MemorySegment x = ((MemorySegment) data.invokeExact((MemorySegment) vec.invokeExact(ch, 0L))).reinterpret(rows * 8);
            MemorySegment s = ((MemorySegment) data.invokeExact((MemorySegment) vec.invokeExact(ch, 1L))).reinterpret(rows * 16);
            for (long r = 0; r < rows; r++) h += x.get(JAVA_LONG, r * 8) + s.get(JAVA_INT, r * 16);
            cp.set(ADDRESS, 0, ch); destroyChunk.invokeExact(cp);
          }
          destroyResult.invokeExact(res);
        }
        System.out.printf("FFM:  %.1f us per small query (h=%d)%n", (System.nanoTime() - t0) / 1e3 / n, h);
      }
    }
  }
}
