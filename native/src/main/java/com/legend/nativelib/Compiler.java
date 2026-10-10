package com.legend.nativelib;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.WordFactory;

import planner.Boundary;
import planner.Folded;

/**
 * legend-lite's compiler as a NATIVE SHARED LIBRARY: PYTHON'S ADAPTER to the boundary the browser runs as
 * WebAssembly ({@code planner.Boundary}, //wasm:boundary) -- each C function a delegation to one of its operations,
 * answered in the text encoding the tab's adapter answers in ({@code planner.Folded}; docs/PROTOCOL_PROGRAM_2026_10_05.md,
 * invariant 5). One compiler source, two builds, held to the same differential corpus (//python:bindings_test).
 * legend-engine's operations Python asks through {@code lite_pure_v1}, as it would ask a server; no function here is
 * its own version of a {@code pure/v1} endpoint.
 *
 * <p>Every function takes C strings (UTF-8) and returns the boundary's answer -- {@code "OK\n<result>"} or
 * {@code "ERR\n<kind>\n<message>"}, a {@code pure/v1} answer {@code "OK\n<status>\n<type>\n<body>"} -- as a NEW C
 * string the caller releases with {@code lite_free}. The first argument is the calling thread's isolate thread:
 * GraalVM requires every OS thread that calls in to have attached itself ({@code graal_attach_thread}).
 *
 * <p>Its own package, not {@code planner}: native-image finds no entry point in a class whose
 * package is split with another jar's (measured, 2026-10-02).
 */
public final class Compiler {

    /** The answers handed out and not yet freed (this isolate's): the library's own accounting, so a host can
     *  show that it frees every answer -- an exact count, where the process's size moves with the heap. */
    private static final AtomicLong UNFREED = new AtomicLong();

    private Compiler() {
    }

    /** A new C string holding {@code s} (UTF-8), the caller's to free with {@code lite_free}. */
    private static CCharPointer answer(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        CCharPointer p = UnmanagedMemory.malloc(WordFactory.unsigned(b.length + 1));
        for (int i = 0; i < b.length; i++) p.write(i, b[i]);
        p.write(b.length, (byte) 0);
        UNFREED.incrementAndGet();
        return p;
    }

    /** A C string as UTF-8 -- named, never the platform's default charset. */
    private static String text(CCharPointer p) {
        int n = 0;
        while (p.read(n) != 0) n++;
        return CTypeConversion.toJavaString(p, WordFactory.unsigned(n), StandardCharsets.UTF_8);
    }

    // Each entry point reads its C strings into Java strings, then delegates. Folded answers the boundary's refusals;
    // what it does not -- a Java Error such as the isolate's heap exhausted, or a class that failed to initialise -- is
    // caught here and answered the same way (Folded.failure): uncaught, GraalVM's default handler would abort the host
    // process. (One helper cannot do this for all: native-image cannot capture a C pointer in a lambda, so each reads
    // its arguments first.)

    /** A lambda's protocol JSON planned -- {@code {"sql","type"}} ({@code Boundary.planJson}). */
    @CEntryPoint(name = "lite_plan_json")
    static CCharPointer planJson(IsolateThread thread, CCharPointer model, CCharPointer lambdaJson, CCharPointer runtime) {
        String a;
        try {
            String m = text(model), l = text(lambdaJson), r = text(runtime);
            a = Folded.of(() -> Boundary.planJson(m, l, r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** Pure TEXT planned (the differential corpus's entry): {@code {"sql","type"}} ({@code Boundary.plan}). */
    @CEntryPoint(name = "lite_plan_text")
    static CCharPointer planText(IsolateThread thread, CCharPointer model, CCharPointer query, CCharPointer runtime) {
        String a;
        try {
            String m = text(model), q = text(query), r = text(runtime);
            a = Folded.of(() -> Boundary.plan(m, q, r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** A Pure Database (model text) written from a table's catalog rows, as DataCube writes one for a file. */
    @CEntryPoint(name = "lite_database_from_catalog")
    static CCharPointer databaseFromCatalog(IsolateThread thread, CCharPointer catalogJson) {
        String a;
        try {
            String c = text(catalogJson);
            a = Folded.of(() -> Boundary.databaseFromCatalog(c));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** The model for a table, from its catalog rows: Database, connection and runtime ({@code Boundary.tableModel}). */
    @CEntryPoint(name = "lite_table_model")
    static CCharPointer tableModel(IsolateThread thread, CCharPointer tableJson) {
        String a;
        try {
            String t = text(tableJson);
            a = Folded.of(() -> Boundary.tableModel(t));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** The catalog question for one table of a DuckDB, its names filled in ({@code Boundary.catalogColumnsSql}). */
    @CEntryPoint(name = "lite_catalog_columns_sql")
    static CCharPointer catalogColumnsSql(IsolateThread thread, CCharPointer schema, CCharPointer table) {
        String a;
        try {
            String s = text(schema), t = text(table);
            a = Folded.of(() -> Boundary.catalogColumnsSql(s, t));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** What a session of the given database runs before it is queried ({@code Boundary.sessionSetup}). */
    @CEntryPoint(name = "lite_session_setup")
    static CCharPointer sessionSetup(IsolateThread thread, CCharPointer databaseType) {
        String a;
        try {
            String d = text(databaseType);
            a = Folded.of(() -> Boundary.sessionSetup(d));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** One legend-engine pure/v1 call by its path and raw query, as legend-lite's server answers it ({@code Boundary.pureV1}). */
    @CEntryPoint(name = "lite_pure_v1")
    static CCharPointer pureV1(IsolateThread thread, CCharPointer path, CCharPointer rawQuery, CCharPointer body) {
        String a;
        try {
            String p = text(path), q = text(rawQuery), b = text(body);
            a = Folded.http(() -> Boundary.pureV1(p, q, b));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** Execute's plan half in upstream's Arrow format: the SQL and the schema metadata ({@code Boundary.executePlan}). */
    @CEntryPoint(name = "lite_execute_plan")
    static CCharPointer executePlan(IsolateThread thread, CCharPointer body, CCharPointer models) {
        String a;
        try {
            String b = text(body), m = text(models);
            a = Folded.http(() -> Boundary.executePlan(b, m));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** A host's refusal of a call it could not finish, in the engine's error shape ({@code Boundary.refusal}). */
    @CEntryPoint(name = "lite_pure_v1_refusal")
    static CCharPointer refusal(IsolateThread thread, CCharPointer message) {
        String a;
        try {
            String m = text(message);
            a = Folded.http(() -> Boundary.refusal(m));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** Releases a string this library returned. */
    @CEntryPoint(name = "lite_free")
    static void free(IsolateThread thread, CCharPointer p) {
        if (p.isNull()) return;
        UnmanagedMemory.free(p);
        UNFREED.decrementAndGet();
    }

    /** How many answers this library has returned that were not yet released with {@code lite_free}. */
    @CEntryPoint(name = "lite_unfreed")
    static long unfreed(IsolateThread thread) {
        return UNFREED.get();
    }
}
