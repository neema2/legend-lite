import static java.lang.foreign.ValueLayout.*;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * Probe: the statement surface a server needs, through DuckDB's C API with FFM (signatures from duckdb.h
 * v1.5.5): error messages and their kinds, cancel from another thread, prepared statements (parameters,
 * columns BEFORE running, binding), multi-statement scripts, write counts. With DuckDB's JDBC driver on the
 * classpath and -Djdbc=true, each error message is printed next to JDBC's for the same SQL.
 */
public class ExecProbe {
    static final Linker L = Linker.nativeLinker();
    static SymbolLookup lib;
    static final StructLayout RESULT = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS);
    static MethodHandle fn(String n, FunctionDescriptor d) { return L.downcallHandle(lib.find(n).orElseThrow(() -> new IllegalStateException(n)), d); }
    static MethodHandle open, connect, query, resultError, errorType, destroyResult, rowsChanged, valueInt64, interrupt,
            prepare, prepareError, nparams, paramType, colCount, colName, bindInt, executePrepared, destroyPrepare,
            extract, extractError, prepareExtracted, destroyExtracted;
    static MemorySegment conn;

    public static void main(String[] args) throws Throwable {
        lib = SymbolLookup.libraryLookup(Path.of(args[0]), Arena.global());
        open = fn("duckdb_open", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        connect = fn("duckdb_connect", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        query = fn("duckdb_query", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        resultError = fn("duckdb_result_error", FunctionDescriptor.of(ADDRESS, ADDRESS));
        errorType = fn("duckdb_result_error_type", FunctionDescriptor.of(JAVA_INT, ADDRESS));
        destroyResult = fn("duckdb_destroy_result", FunctionDescriptor.ofVoid(ADDRESS));
        rowsChanged = fn("duckdb_rows_changed", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        valueInt64 = fn("duckdb_value_int64", FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));
        interrupt = fn("duckdb_interrupt", FunctionDescriptor.ofVoid(ADDRESS));
        prepare = fn("duckdb_prepare", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        prepareError = fn("duckdb_prepare_error", FunctionDescriptor.of(ADDRESS, ADDRESS));
        nparams = fn("duckdb_nparams", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        paramType = fn("duckdb_param_type", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG));
        colCount = fn("duckdb_prepared_statement_column_count", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        colName = fn("duckdb_prepared_statement_column_name", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        bindInt = fn("duckdb_bind_int32", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT));
        executePrepared = fn("duckdb_execute_prepared", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        destroyPrepare = fn("duckdb_destroy_prepare", FunctionDescriptor.ofVoid(ADDRESS));
        extract = fn("duckdb_extract_statements", FunctionDescriptor.of(JAVA_LONG, ADDRESS, ADDRESS, ADDRESS));
        extractError = fn("duckdb_extract_statements_error", FunctionDescriptor.of(ADDRESS, ADDRESS));
        prepareExtracted = fn("duckdb_prepare_extracted_statement", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS));
        destroyExtracted = fn("duckdb_destroy_extracted", FunctionDescriptor.ofVoid(ADDRESS));

        Arena g = Arena.global();
        MemorySegment db = g.allocate(ADDRESS), c = g.allocate(ADDRESS);
        int s1 = (int) open.invokeExact(MemorySegment.NULL, db);
        int s2 = (int) connect.invokeExact(db.get(ADDRESS, 0), c);
        conn = c.get(ADDRESS, 0);
        java.sql.Connection jdbc = Boolean.getBoolean("jdbc") ? java.sql.DriverManager.getConnection("jdbc:duckdb:") : null;
        for (java.sql.Connection x : jdbc == null ? new java.sql.Connection[0] : new java.sql.Connection[] {jdbc}) {
            try (var st = x.createStatement()) { st.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)"); }
        }
        run("CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)");

        System.out.println("== errors (C API kind and message; JDBC's message beneath)");
        String[] bad = {"SELEC 1", "SELECT * FROM nope", "SELECT nope FROM t", "SELECT 'x'::INTEGER",
                "INSERT INTO t VALUES (1, NULL)", "SELECT error('custom failure')"};
        for (String sql : bad) {
            String[] e = run(sql);
            System.out.println("  " + sql + "\n     C:    [" + e[0] + "] " + e[1].lines().findFirst().orElse(""));
            if (jdbc != null) {
                try (var p = jdbc.prepareStatement(sql)) { p.execute(); System.out.println("     JDBC: (no error)"); }
                catch (java.sql.SQLException je) { System.out.println("     JDBC: " + je.getMessage().lines().findFirst().orElse("")); }
            }
        }

        System.out.println("== write counts");
        System.out.println("  INSERT 3 rows -> rows_changed=" + run("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")[2]);
        System.out.println("  UPDATE 2 rows -> rows_changed=" + run("UPDATE t SET name = 'z' WHERE id < 3")[2]);
        System.out.println("  CREATE VIEW   -> rows_changed=" + run("CREATE VIEW v AS SELECT 1")[2]);

        System.out.println("== prepared statement: parameters and columns before running");
        try (Arena a = Arena.ofConfined()) {
            MemorySegment ps = a.allocate(ADDRESS);
            int st = (int) prepare.invokeExact(conn, a.allocateFrom("SELECT id, name, ?::INTEGER + 1 AS next FROM t WHERE id >= ?"), ps);
            MemorySegment p = ps.get(ADDRESS, 0);
            long np = (long) nparams.invokeExact(p);
            long nc = (long) colCount.invokeExact(p);
            StringBuilder cols = new StringBuilder();
            for (long i = 0; i < nc; i++) cols.append(i > 0 ? ", " : "").append(((MemorySegment) colName.invokeExact(p, i)).reinterpret(Long.MAX_VALUE).getString(0));
            System.out.println("  prepare state=" + st + ", " + np + " params (types " + (int) paramType.invokeExact(p, 1L) + ", " + (int) paramType.invokeExact(p, 2L) + "), columns [" + cols + "]");
            int b1 = (int) bindInt.invokeExact(p, 1L, 41);
            int b2 = (int) bindInt.invokeExact(p, 2L, 2);
            MemorySegment r = a.allocate(RESULT);
            int ex = (int) executePrepared.invokeExact(p, r);
            System.out.println("  executed: state=" + ex + ", first row id=" + (long) valueInt64.invokeExact(r, 0L, 0L) + " next=" + (long) valueInt64.invokeExact(r, 2L, 0L));
            destroyResult.invokeExact(r);
            destroyPrepare.invokeExact(ps);
            int bad2 = (int) prepare.invokeExact(conn, a.allocateFrom("SELECT * FROM nope"), ps);
            System.out.println("  prepare of a bad statement: state=" + bad2 + " error=" + ((MemorySegment) prepareError.invokeExact(ps.get(ADDRESS, 0))).reinterpret(Long.MAX_VALUE).getString(0).lines().findFirst().orElse(""));
            destroyPrepare.invokeExact(ps);
        }

        System.out.println("== a script: split by DuckDB's own parser, each statement run in turn");
        try (Arena a = Arena.ofConfined()) {
            MemorySegment ex = a.allocate(ADDRESS);
            long n = (long) extract.invokeExact(conn, a.allocateFrom("CREATE TEMP TABLE s AS SELECT 1 AS x; INSERT INTO s VALUES (2); SELECT sum(x) FROM s"), ex);
            System.out.println("  " + n + " statements");
            for (long i = 0; i < n; i++) {
                MemorySegment ps = a.allocate(ADDRESS), r = a.allocate(RESULT);
                int st = (int) prepareExtracted.invokeExact(conn, ex.get(ADDRESS, 0), i, ps);
                int e2 = (int) executePrepared.invokeExact(ps.get(ADDRESS, 0), r);
                if (i == n - 1) System.out.println("  last statement's answer: " + (long) valueInt64.invokeExact(r, 0L, 0L));
                destroyResult.invokeExact(r);
                destroyPrepare.invokeExact(ps);
            }
            destroyExtracted.invokeExact(ex);
            long bad3 = (long) extract.invokeExact(conn, a.allocateFrom("SELECT 1; SELEC 2"), ex);
            System.out.println("  a script with a syntax error: " + bad3 + " statements, error=" + ((MemorySegment) extractError.invokeExact(ex.get(ADDRESS, 0))).reinterpret(Long.MAX_VALUE).getString(0).lines().findFirst().orElse(""));
            destroyExtracted.invokeExact(ex);
        }

        System.out.println("== cancel: a long query, interrupted from another thread after 300 ms");
        Thread killer = new Thread(() -> {
            try { Thread.sleep(300); interrupt.invokeExact(conn); } catch (Throwable e) { throw new RuntimeException(e); }
        });
        long t0 = System.nanoTime();
        killer.start();
        String[] e = run("SELECT count(*) FROM range(100000000000) t(i) WHERE i % 7 = 3");
        System.out.printf("  returned after %d ms: [%s] %s%n", (System.nanoTime() - t0) / 1_000_000, e[0], e[1]);
        System.out.println("  the connection afterwards: " + run("SELECT 42")[2].isEmpty() + " (usable: " + (run("SELECT 42")[0].equals("ok")) + ")");
    }

    /** {kind or "ok", message, rows changed}. */
    static String[] run(String sql) throws Throwable {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment r = a.allocate(RESULT);
            int st = (int) query.invokeExact(conn, a.allocateFrom(sql), r);
            String[] out;
            if (st == 0) out = new String[] {"ok", "", Long.toString((long) rowsChanged.invokeExact(r))};
            else out = new String[] {kind((int) errorType.invokeExact(r)), ((MemorySegment) resultError.invokeExact(r)).reinterpret(Long.MAX_VALUE).getString(0), ""};
            destroyResult.invokeExact(r);
            return out;
        }
    }

    static String kind(int t) {
        return switch (t) { case 2 -> "CONVERSION"; case 13 -> "CATALOG"; case 14 -> "PARSER"; case 18 -> "CONSTRAINT"; case 24 -> "BINDER"; case 29 -> "INTERRUPT"; default -> "type " + t; };
    }
}
