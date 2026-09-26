package com.legend.warehouse.server.duck;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import com.legend.Nullable;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code system.main.authenticated_user()}: who the server verified, read from
 * OUTSIDE SQL (docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md §5). DuckDB calls its
 * bind once per query on the calling query's own client context; the context
 * gives the connection id; the server's map gives the principal. No statement
 * can set it, because it is not stored anywhere SQL can write. A connection the
 * server never mapped fails the query at bind: fail closed.
 *
 * <p>Views must call it fully qualified: an unqualified name is shadowed by a
 * user's TEMP MACRO of the same name (measured). {@code current_user} and
 * {@code session_user} are per-connection temp macros over it, a display
 * convenience no view reads (DuckDB's own are internal and cannot be replaced).
 *
 * <p>The three callbacks are Java methods DuckDB calls (FFM upcalls, bound once
 * through {@link MethodHandles.Lookup}: no reflection). An exception must never
 * escape one into DuckDB's native code: each reports failure to DuckDB instead.
 */
final class AuthenticatedUser {

    static final String NAME = "authenticated_user";

    private AuthenticatedUser() {
    }

    /** Per database (by its number): connection id to principal. Written only by the server. */
    private static final Map<Long, Map<Long, String>> PRINCIPALS = new ConcurrentHashMap<>();
    private static final AtomicLong NUMBERS = new AtomicLong();
    /** Each principal's name as a C string, for the life of the process (one per user). */
    private static final Map<String, MemorySegment> C_NAMES = new ConcurrentHashMap<>();

    private record Stubs(MemorySegment bind, MemorySegment execute, MemorySegment copy) {
    }

    private static volatile @Nullable Stubs stubs;

    private static Stubs stubs() throws ReflectiveOperationException {
        Stubs s = stubs;
        if (s != null) return s;
        synchronized (AuthenticatedUser.class) {
            s = stubs;
            if (s == null) {
                MethodHandles.Lookup here = MethodHandles.lookup();
                s = new Stubs(
                        Duck.LINKER.upcallStub(here.findStatic(AuthenticatedUser.class, "onBind",
                                MethodType.methodType(void.class, MemorySegment.class)),
                                FunctionDescriptor.ofVoid(ADDRESS), Arena.global()),
                        Duck.LINKER.upcallStub(here.findStatic(AuthenticatedUser.class, "onExecute",
                                MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class, MemorySegment.class)),
                                FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, ADDRESS), Arena.global()),
                        Duck.LINKER.upcallStub(here.findStatic(AuthenticatedUser.class, "onCopy",
                                MethodType.methodType(MemorySegment.class, MemorySegment.class)),
                                FunctionDescriptor.of(ADDRESS, ADDRESS), Arena.global()));
                stubs = s;
            }
        }
        return s;
    }

    /** Registers the function in the database behind {@code conn}; the database's number for {@link #bind}. */
    static long register(Duck d, MemorySegment conn) {
        long number = NUMBERS.incrementAndGet();
        PRINCIPALS.put(number, new ConcurrentHashMap<>());
        try (Arena a = Arena.ofConfined()) {
            Stubs s = stubs();
            MemorySegment f = (MemorySegment) d.createScalarFunction.invokeExact();
            d.setName.invokeExact(f, a.allocateFrom(NAME));
            MemorySegment varchar = (MemorySegment) d.createLogicalType.invokeExact(Duck.VARCHAR);
            d.setReturnType.invokeExact(f, varchar);
            MemorySegment vp = a.allocate(ADDRESS);
            vp.set(ADDRESS, 0, varchar);
            d.destroyLogicalType.invokeExact(vp);
            d.setVolatile.invokeExact(f);   // never folded into a plan or shared between callers
            d.setBind.invokeExact(f, s.bind());
            d.setFunction.invokeExact(f, s.execute());
            MemorySegment info = Arena.global().allocate(JAVA_LONG);   // lives as long as the function
            info.set(JAVA_LONG, 0, number);
            d.setExtraInfo.invokeExact(f, info, MemorySegment.NULL);
            int state = (int) d.registerScalarFunction.invokeExact(conn, f);
            MemorySegment fp = a.allocate(ADDRESS);
            fp.set(ADDRESS, 0, f);
            d.destroyScalarFunction.invokeExact(fp);
            if (state != 0) throw new IllegalStateException("could not register " + NAME);
            return number;
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    static void bind(long database, long connection, String principal) {
        Map<Long, String> m = PRINCIPALS.get(database);
        if (m == null) throw new IllegalStateException("no identity function in database " + database);
        m.put(connection, principal);
    }

    static void unbind(long database, long connection) {
        Map<Long, String> m = PRINCIPALS.get(database);
        if (m != null) m.remove(connection);
    }

    static void forget(long database) {
        PRINCIPALS.remove(database);
    }

    /** BIND (once per query, the caller's context): connection id -> principal, kept as bind data. */
    private static void onBind(MemorySegment info) {
        Duck d = Duck.api();
        try (Arena a = Arena.ofConfined()) {
            try {
                long database = ((MemorySegment) d.bindExtraInfo.invokeExact(info)).reinterpret(8).get(JAVA_LONG, 0);
                MemorySegment ctx = a.allocate(ADDRESS);
                d.bindContext.invokeExact(info, ctx);
                long connection = (long) d.contextConnectionId.invokeExact(ctx.get(ADDRESS, 0));
                d.destroyContext.invokeExact(ctx);
                Map<Long, String> m = PRINCIPALS.get(database);
                String who = m == null ? null : m.get(connection);
                if (who == null) {
                    d.bindSetError.invokeExact(info, a.allocateFrom("no authenticated user for this connection"));
                    return;
                }
                d.setBindData.invokeExact(info, C_NAMES.computeIfAbsent(who, w -> Arena.global().allocateFrom(w)),
                        MemorySegment.NULL);
                d.setBindDataCopy.invokeExact(info, stubs().copy());
            } catch (Throwable t) {
                try {
                    d.bindSetError.invokeExact(info, a.allocateFrom("authenticated_user: " + t));
                } catch (Throwable ignored) {
                    // nothing more can be reported from inside DuckDB
                }
            }
        }
    }

    /** The bind data is a process-lifetime string: a copy is the same pointer. */
    private static MemorySegment onCopy(MemorySegment data) {
        return data;
    }

    /** EXECUTE (per chunk, maybe on a worker thread): the bound principal in every row. */
    private static void onExecute(MemorySegment info, MemorySegment input, MemorySegment output) {
        Duck d = Duck.api();
        try {
            MemorySegment who = (MemorySegment) d.getBindData.invokeExact(info);
            long n = (long) d.chunkSize.invokeExact(input);
            for (long i = 0; i < n; i++) d.assignString.invokeExact(output, i, who);
        } catch (Throwable t) {
            try (Arena a = Arena.ofConfined()) {
                d.setError.invokeExact(info, a.allocateFrom("authenticated_user: " + t));
            } catch (Throwable ignored) {
                // nothing more can be reported from inside DuckDB
            }
        }
    }
}
