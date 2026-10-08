package com.legend.nativelib;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.WordFactory;

import planner.Wasm;

/**
 * legend-lite's compiler as a NATIVE SHARED LIBRARY: the very entry points the WebAssembly planner
 * exports (planner.Wasm, //wasm:boundary), as C functions -- one compiler source, two builds, held
 * to the same differential corpus (//python:bindings_test).
 *
 * <p>Every function takes C strings (UTF-8) and returns the planner's own answer -- {@code "OK\n<result>"} (JSON,
 * or Pure text for {@code lite_compose}) or {@code "ERR\n<class>\n<message>"} -- as a NEW C string the caller
 * releases with {@code lite_free}. The first argument is the calling thread's isolate thread: GraalVM requires
 * every OS thread that calls in to have attached itself ({@code graal_attach_thread}).
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

    /**
     * A failure the planner does not answer itself -- a Java {@code Error} such as the isolate's heap exhausted, or a
     * class that failed to initialise -- as the same {@code ERR} answer the planner gives for its own refusals. Each
     * entry point catches it: uncaught, GraalVM's default handler would abort the host process. (A lambda cannot do
     * this once for all: native-image cannot capture a C pointer in one.)
     */
    private static String failed(Throwable failure) {
        String message = failure.getMessage() == null ? "" : failure.getMessage();
        return "ERR\n" + failure.getClass().getName() + "\n" + message;
    }

    /** A lambda's protocol JSON planned -- {@code {"sql","type"}}. */
    @CEntryPoint(name = "lite_plan_json")
    static CCharPointer planJson(IsolateThread thread, CCharPointer model, CCharPointer lambdaJson, CCharPointer runtime) {
        String a;
        try {
            a = Wasm.planJsonOrError(text(model), text(lambdaJson), text(runtime));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** Pure TEXT planned (the differential corpus's entry): {@code {"sql","type"}}. */
    @CEntryPoint(name = "lite_plan_text")
    static CCharPointer planText(IsolateThread thread, CCharPointer model, CCharPointer query, CCharPointer runtime) {
        String a;
        try {
            a = Wasm.planOrError(text(model), text(query), text(runtime));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** A lambda's protocol JSON typed, compile-only -- its {@code RelationType}. */
    @CEntryPoint(name = "lite_relation_type_json")
    static CCharPointer relationTypeJson(IsolateThread thread, CCharPointer model, CCharPointer lambdaJson) {
        String a;
        try {
            a = Wasm.relationTypeJsonOrError(text(model), text(lambdaJson));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** Pure text as its lambda's protocol JSON. */
    @CEntryPoint(name = "lite_lambda_json")
    static CCharPointer lambdaJson(IsolateThread thread, CCharPointer pureText) {
        String a;
        try {
            a = Wasm.lambdaJsonOrError(text(pureText));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** A lambda's protocol JSON as Pure text, {@code STANDARD} or {@code PRETTY}. */
    @CEntryPoint(name = "lite_compose")
    static CCharPointer compose(IsolateThread thread, CCharPointer lambdaJson, CCharPointer style) {
        String a;
        try {
            a = Wasm.composeLambdaOrError(text(lambdaJson), text(style));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** A model's text as its elements (PureModelContextData JSON). */
    @CEntryPoint(name = "lite_model_json")
    static CCharPointer modelJson(IsolateThread thread, CCharPointer modelText) {
        String a;
        try {
            a = Wasm.modelJsonOrError(text(modelText));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** A Pure Database (model text) written from a table's catalog rows, as DataCube writes one for a file. */
    @CEntryPoint(name = "lite_database_from_catalog")
    static CCharPointer databaseFromCatalog(IsolateThread thread, CCharPointer catalogJson) {
        String a;
        try {
            a = Wasm.databaseFromCatalogOrError(text(catalogJson));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** The model for a table, from its catalog rows: Database, connection and runtime (Wasm.tableModelOrError). */
    @CEntryPoint(name = "lite_table_model")
    static CCharPointer tableModel(IsolateThread thread, CCharPointer tableJson) {
        String a;
        try {
            a = Wasm.tableModelOrError(text(tableJson));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** The catalog question for one table of a DuckDB, its names filled in (Wasm.catalogColumnsSqlOrError). */
    @CEntryPoint(name = "lite_catalog_columns_sql")
    static CCharPointer catalogColumnsSql(IsolateThread thread, CCharPointer schema, CCharPointer table) {
        String a;
        try {
            a = Wasm.catalogColumnsSqlOrError(text(schema), text(table));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** What a session of the given database runs before it is queried (Wasm.sessionSetupOrError). */
    @CEntryPoint(name = "lite_session_setup")
    static CCharPointer sessionSetup(IsolateThread thread, CCharPointer databaseType) {
        String a;
        try {
            a = Wasm.sessionSetupOrError(text(databaseType));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** One legend-engine pure/v1 call by its path and raw query, as legend-lite's server answers it (Wasm.pureV1OrError). */
    @CEntryPoint(name = "lite_pure_v1")
    static CCharPointer pureV1(IsolateThread thread, CCharPointer path, CCharPointer rawQuery, CCharPointer body) {
        String a;
        try {
            a = Wasm.pureV1OrError(text(path), text(rawQuery), text(body));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** Execute's plan half in upstream's Arrow format: the SQL and the schema metadata (Wasm.executePlanOrError). */
    @CEntryPoint(name = "lite_execute_plan")
    static CCharPointer executePlan(IsolateThread thread, CCharPointer body, CCharPointer models) {
        String a;
        try {
            a = Wasm.executePlanOrError(text(body), text(models));
        } catch (Throwable failure) {
            a = failed(failure);
        }
        return answer(a);
    }

    /** A host's refusal of a call it could not finish, in the engine's error shape (Wasm.refusalOrError). */
    @CEntryPoint(name = "lite_pure_v1_refusal")
    static CCharPointer refusal(IsolateThread thread, CCharPointer message) {
        String a;
        try {
            a = Wasm.refusalOrError(text(message));
        } catch (Throwable failure) {
            a = failed(failure);
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
