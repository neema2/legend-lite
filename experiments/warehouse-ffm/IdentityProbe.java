import static java.lang.foreign.ValueLayout.*;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Probe: the principal from OUTSIDE SQL. app_user() is a scalar function the server registers through
 * DuckDB's C API, implemented in Java (an FFM upcall). DuckDB hands it the calling query's client
 * context; the context gives the connection id; the server's own map gives the principal. No SQL
 * statement can set it, because it is not stored anywhere SQL can write.
 */
public class IdentityProbe {
    static final Linker L = Linker.nativeLinker();
    static SymbolLookup lib;
    static final StructLayout RESULT = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS);
    /** The server's map: connection id -> principal. Written only by the server. */
    static final Map<Long, String> PRINCIPALS = new ConcurrentHashMap<>();
    static final Map<String, MemorySegment> NAMES = new ConcurrentHashMap<>();
    static volatile long calls;

    static MethodHandle fn(String name, FunctionDescriptor d) {
        return L.downcallHandle(lib.find(name).orElseThrow(() -> new IllegalStateException("no symbol " + name)), d);
    }

    static MethodHandle open, connect, query, resultError, destroyResult, rowCount, valueVarchar, valueInt64, free,
            connContext, contextConnId, destroyContext, fnContext, chunkSize, assignString, setError,
            bindSetError, setBindData, setBindDataCopy, getBindData;

    public static void main(String[] a) throws Throwable {
        lib = SymbolLookup.libraryLookup(Path.of(a[0]), Arena.global());
        open = fn("duckdb_open", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        connect = fn("duckdb_connect", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        query = fn("duckdb_query", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
        resultError = fn("duckdb_result_error", FunctionDescriptor.of(ADDRESS, ADDRESS));
        destroyResult = fn("duckdb_destroy_result", FunctionDescriptor.ofVoid(ADDRESS));
        rowCount = fn("duckdb_row_count", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        valueVarchar = fn("duckdb_value_varchar", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG, JAVA_LONG));
        valueInt64 = fn("duckdb_value_int64", FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));
        free = fn("duckdb_free", FunctionDescriptor.ofVoid(ADDRESS));
        connContext = fn("duckdb_connection_get_client_context", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        contextConnId = fn("duckdb_client_context_get_connection_id", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        destroyContext = fn("duckdb_destroy_client_context", FunctionDescriptor.ofVoid(ADDRESS));
        fnContext = fn("duckdb_scalar_function_get_client_context", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        chunkSize = fn("duckdb_data_chunk_get_size", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
        assignString = fn("duckdb_vector_assign_string_element", FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG, ADDRESS));
        setError = fn("duckdb_scalar_function_set_error", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        bindSetError = fn("duckdb_scalar_function_bind_set_error", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        setBindData = fn("duckdb_scalar_function_set_bind_data", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, ADDRESS));
        setBindDataCopy = fn("duckdb_scalar_function_set_bind_data_copy", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        getBindData = fn("duckdb_scalar_function_get_bind_data", FunctionDescriptor.of(ADDRESS, ADDRESS));

        Arena arena = Arena.global();
        MemorySegment db = arena.allocate(ADDRESS);
        check((int) open.invokeExact(MemorySegment.NULL, db), "open");
        MemorySegment owner = connectTo(arena, db), alice = connectTo(arena, db), bob = connectTo(arena, db), stranger = connectTo(arena, db);
        PRINCIPALS.put(connId(arena, owner), "owner");
        PRINCIPALS.put(connId(arena, alice), "alice");
        PRINCIPALS.put(connId(arena, bob), "bob");
        System.out.println("connection ids: owner=" + connId(arena, owner) + " alice=" + connId(arena, alice)
                + " bob=" + connId(arena, bob) + " stranger=" + connId(arena, stranger) + " -> map " + PRINCIPALS);

        if (Boolean.getBoolean("standard")) {
            // The standard name. DuckDB's current_user / session_user / current_role are placeholder macros
            // returning 'duckdb'; ours has to take the name.
            String err = tryRegister(arena, owner, "current_user");
            System.out.println("register current_user over DuckDB's macro: " + (err == null ? "ok" : err));
            for (String m : new String[] {"current_user", "session_user", "current_role"}) tryExec(arena, owner, "DROP MACRO system.main." + m);
            err = tryRegister(arena, owner, "current_user");
            System.out.println("register current_user after dropping the macro: " + (err == null ? "ok" : err));
            tryExec(arena, owner, "CREATE MACRO system.main.session_user() AS system.main.current_user()");
            for (String c : new String[] {"alice", "bob"}) {
                MemorySegment conn = c.equals("alice") ? alice : bob;
                System.out.println("   " + c + ": " + row(arena, conn, "SELECT current_user, current_user(), user"));
            }
            System.out.println("   bob: " + row(arena, bob, "SELECT session_user, system.main.current_user(), 'x'"));
            tryExec(arena, bob, "CREATE TEMP MACRO current_user() AS 'alice'");
            System.out.println("   bob after TEMP MACRO current_user(): " + row(arena, bob, "SELECT current_user, system.main.current_user(), 'x'"));
            return;
        }
        registerAppUser(arena, owner);
        exec(arena, owner, "SET threads=4");
        exec(arena, owner, "CREATE TABLE trades AS SELECT i AS id, CASE i % 3 WHEN 0 THEN 'EMEA' WHEN 1 THEN 'APAC' ELSE 'AMER' END AS region, i * 1.0 AS amount FROM range(1000000) t(i)");
        exec(arena, owner, "CREATE TABLE acl(username VARCHAR, region VARCHAR)");
        exec(arena, owner, "INSERT INTO acl VALUES ('alice', 'EMEA'), ('bob', 'APAC'), ('bob', 'AMER')");
        exec(arena, owner, "CREATE VIEW secure_trades AS SELECT * FROM trades t WHERE EXISTS (SELECT 1 FROM acl a WHERE a.username = app_user() AND a.region = t.region)");
        exec(arena, owner, "CREATE VIEW secure_trades_q AS SELECT * FROM trades t WHERE EXISTS (SELECT 1 FROM acl a WHERE a.username = system.main.app_user() AND a.region = t.region)");
        exec(arena, owner, "CREATE VIEW secure_trades_var AS SELECT * FROM trades t WHERE EXISTS (SELECT 1 FROM acl a WHERE a.username = getvariable('app_user') AND a.region = t.region)");

        String q = "SELECT app_user() AS who, count(*) AS n, string_agg(DISTINCT region, ',' ORDER BY region) AS regions FROM secure_trades";
        System.out.println("alice:    " + row(arena, alice, q));
        System.out.println("bob:      " + row(arena, bob, q));
        System.out.println("stranger: " + row(arena, stranger, q) + "   (no mapping: fails closed)");
        // spoof attempts, as bob
        String qq = q.replace("FROM secure_trades", "FROM secure_trades_q").replace("app_user()", "system.main.app_user()");
        String qv = "SELECT getvariable('app_user') AS who, count(*) AS n, string_agg(DISTINCT region, ',' ORDER BY region) AS regions FROM secure_trades_var";
        System.out.println("-- spoof attempts on bob's connection:");
        tryExec(arena, bob, "SET VARIABLE app_user = 'alice'");
        System.out.println("   after SET VARIABLE: app_user() view " + row(arena, bob, q) + "  || getvariable view " + row(arena, bob, qv));
        tryExec(arena, bob, "CREATE TEMP MACRO app_user() AS 'alice'");
        System.out.println("   after TEMP MACRO app_user(): unqualified view " + row(arena, bob, q));
        System.out.println("                                qualified view   " + row(arena, bob, qq));
        tryExec(arena, bob, "SET VARIABLE app_user = 'bob'");
        tryExec(arena, bob, "CREATE TEMP MACRO getvariable(x) AS 'alice'");
        System.out.println("   after TEMP MACRO getvariable(x): getvariable view " + row(arena, bob, qv));
        tryExec(arena, bob, "DROP MACRO temp.main.app_user");
        System.out.println("-- " + binds + " binds, " + calls + " executes so far");
        // concurrency: both users at once, many times
        long t0 = System.nanoTime();
        Thread ta = new Thread(() -> loop(arena, alice, q, "alice", 333334));
        Thread tb = new Thread(() -> loop(arena, bob, q, "bob", 666666));
        ta.start(); tb.start(); ta.join(); tb.join();
        System.out.printf("-- 2 x 50 concurrent full scans, every answer the caller's own: %d ms, %d binds, %d executes%n",
                (System.nanoTime() - t0) / 1_000_000, binds, calls);
    }

    static void loop(Arena arena, MemorySegment conn, String q, String who, long expect) {
        try {
            for (int i = 0; i < 50; i++) {
                String r = row(arena, conn, q);
                if (!r.startsWith(who + " | " + expect + " |")) throw new AssertionError(who + " saw " + r);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * BIND, once per query, on the calling query's own client context (duckdb.h: the client context
     * comes from the BIND info, not the per-chunk execute info): connection id -> principal, kept as the
     * function's bind data. A connection the server never mapped fails the query here.
     */
    static void bindAppUser(MemorySegment bindInfo) {
        binds++;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment ctx = a.allocate(ADDRESS);
            fnContext.invokeExact(bindInfo, ctx);
            long id = (long) contextConnId.invokeExact(ctx.get(ADDRESS, 0));
            destroyContext.invokeExact(ctx);
            String who = PRINCIPALS.get(id);
            if (who == null) {
                bindSetError.invokeExact(bindInfo, a.allocateFrom("no principal for connection " + id));
                return;
            }
            MemorySegment name = NAMES.computeIfAbsent(who, w -> Arena.global().allocateFrom(w));
            setBindData.invokeExact(bindInfo, name, MemorySegment.NULL);   // lives for the process: nothing to destroy
            setBindDataCopy.invokeExact(bindInfo, COPY_STUB);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /** The bind data is a process-lifetime string: a copy is the same pointer. */
    static MemorySegment copyBindData(MemorySegment data) {
        return data;
    }

    /** EXECUTE, per chunk (possibly on a worker thread): write the bound principal. */
    static void appUser(MemorySegment info, MemorySegment input, MemorySegment output) {
        calls++;
        try {
            MemorySegment name = (MemorySegment) getBindData.invokeExact(info);
            long n = (long) chunkSize.invokeExact(input);
            for (long i = 0; i < n; i++) assignString.invokeExact(output, i, name);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    static volatile long binds;
    static MemorySegment COPY_STUB;

    static String tryRegister(Arena arena, MemorySegment conn, String name) throws Throwable {
        try {
            registerAppUser(arena, conn, name);
            return null;
        } catch (IllegalStateException e) {
            return e.getMessage();
        }
    }

    static void registerAppUser(Arena arena, MemorySegment conn) throws Throwable {
        registerAppUser(arena, conn, "app_user");
    }

    static void registerAppUser(Arena arena, MemorySegment conn, String fname) throws Throwable {
        MethodHandle create = fn("duckdb_create_scalar_function", FunctionDescriptor.of(ADDRESS));
        MethodHandle setName = fn("duckdb_scalar_function_set_name", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        MethodHandle logical = fn("duckdb_create_logical_type", FunctionDescriptor.of(ADDRESS, JAVA_INT));
        MethodHandle setReturn = fn("duckdb_scalar_function_set_return_type", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        MethodHandle setVolatile = fn("duckdb_scalar_function_set_volatile", FunctionDescriptor.ofVoid(ADDRESS));
        MethodHandle setFunction = fn("duckdb_scalar_function_set_function", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        MethodHandle register = fn("duckdb_register_scalar_function", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        MemorySegment f = (MemorySegment) create.invokeExact();
        setName.invokeExact(f, arena.allocateFrom(fname));
        setReturn.invokeExact(f, (MemorySegment) logical.invokeExact(17));   // DUCKDB_TYPE_VARCHAR
        setVolatile.invokeExact(f);   // never folded into a plan, never cached across callers
        MethodHandle target = MethodHandles.lookup().findStatic(IdentityProbe.class, "appUser",
                MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class, MemorySegment.class));
        MemorySegment stub = L.upcallStub(target, FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, ADDRESS), Arena.global());
        setFunction.invokeExact(f, stub);
        MethodHandle setBind = fn("duckdb_scalar_function_set_bind", FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));
        MemorySegment bindStub = L.upcallStub(MethodHandles.lookup().findStatic(IdentityProbe.class, "bindAppUser",
                MethodType.methodType(void.class, MemorySegment.class)), FunctionDescriptor.ofVoid(ADDRESS), Arena.global());
        setBind.invokeExact(f, bindStub);
        COPY_STUB = L.upcallStub(MethodHandles.lookup().findStatic(IdentityProbe.class, "copyBindData",
                MethodType.methodType(MemorySegment.class, MemorySegment.class)), FunctionDescriptor.of(ADDRESS, ADDRESS), Arena.global());
        check((int) register.invokeExact(conn, f), "register app_user");
    }

    static MemorySegment connectTo(Arena arena, MemorySegment db) throws Throwable {
        MemorySegment c = arena.allocate(ADDRESS);
        check((int) connect.invokeExact(db.get(ADDRESS, 0), c), "connect");
        return c.get(ADDRESS, 0);
    }

    static long connId(Arena arena, MemorySegment conn) throws Throwable {
        MemorySegment ctx = arena.allocate(ADDRESS);
        connContext.invokeExact(conn, ctx);
        long id = (long) contextConnId.invokeExact(ctx.get(ADDRESS, 0));
        destroyContext.invokeExact(ctx);
        return id;
    }

    static void check(int st, String what) {
        if (st != 0) throw new IllegalStateException(what + " failed");
    }

    static void exec(Arena arena, MemorySegment conn, String sql) throws Throwable {
        String e = tryExec(arena, conn, sql);
        if (e != null) throw new IllegalStateException(sql + ": " + e);
    }

    static String tryExec(Arena arena, MemorySegment conn, String sql) throws Throwable {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment res = a.allocate(RESULT);
            int st = (int) query.invokeExact(conn, a.allocateFrom(sql), res);
            String err = st == 0 ? null : ((MemorySegment) resultError.invokeExact(res)).reinterpret(Long.MAX_VALUE).getString(0);
            destroyResult.invokeExact(res);
            if (err != null && !sql.startsWith("CREATE TABLE") && !sql.startsWith("CREATE VIEW") && !sql.startsWith("INSERT") && !sql.startsWith("SET threads"))
                System.out.println("   refused: " + sql + " -> " + err.lines().findFirst().orElse(""));
            else if (err == null && !sql.startsWith("CREATE TABLE") && !sql.startsWith("CREATE VIEW") && !sql.startsWith("INSERT") && !sql.startsWith("SET threads"))
                System.out.println("   accepted: " + sql);
            return err;
        }
    }

    /** The first row, as text joined with " | ". */
    static String row(Arena arena, MemorySegment conn, String sql) throws Throwable {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment res = a.allocate(RESULT);
            int st = (int) query.invokeExact(conn, a.allocateFrom(sql), res);
            if (st != 0) {
                String err = ((MemorySegment) resultError.invokeExact(res)).reinterpret(Long.MAX_VALUE).getString(0);
                destroyResult.invokeExact(res);
                return "ERROR " + err.lines().findFirst().orElse("");
            }
            StringBuilder sb = new StringBuilder();
            for (long c = 0; c < 3; c++) {
                MemorySegment v = (MemorySegment) valueVarchar.invokeExact(res, c, 0L);
                sb.append(c > 0 ? " | " : "").append(v.equals(MemorySegment.NULL) ? "NULL" : v.reinterpret(Long.MAX_VALUE).getString(0));
                if (!v.equals(MemorySegment.NULL)) free.invokeExact(v);
            }
            destroyResult.invokeExact(res);
            return sb.toString();
        }
    }
}
