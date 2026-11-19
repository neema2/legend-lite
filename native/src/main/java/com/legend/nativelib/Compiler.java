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

    /** A new C string holding {@code s} (UTF-8), the caller's to free with {@code lite_free}. Python's model home's
     *  answers too ({@link ModelHome}), and the edge's saved queries' (edge.nativelib.SavedQueriesHome: public since
     *  2026-11-17, the separation census's A2, its upstream path the edge's). */
    public static CCharPointer answer(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        CCharPointer p = UnmanagedMemory.malloc(WordFactory.unsigned(b.length + 1));
        for (int i = 0; i < b.length; i++) p.write(i, b[i]);
        p.write(b.length, (byte) 0);
        UNFREED.incrementAndGet();
        return p;
    }

    /** A C string as UTF-8 -- named, never the platform's default charset. */
    public static String text(CCharPointer p) {
        int n = 0;
        while (p.read(n) != 0) n++;
        return CTypeConversion.toJavaString(p, WordFactory.unsigned(n), StandardCharsets.UTF_8);
    }

    // Each entry point reads its C strings into Java strings, then delegates. Folded answers the boundary's refusals;
    // what it does not -- a Java Error such as the isolate's heap exhausted, or a class that failed to initialise -- is
    // caught here and answered the same way (Folded.failure): uncaught, GraalVM's default handler would abort the host
    // process. (One helper cannot do this for all: native-image cannot capture a C pointer in a lambda, so each reads
    // its arguments first.)



    // ---- models held: compiled once, planned against many times (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §9). A
    // handle travels as its digits, as every argument here is a C string.

    /** {@code Boundary.holdModel}: the handle that names {@code model}, compiled the first time it is held. */
    @CEntryPoint(name = "lite_hold_model")
    static CCharPointer holdModel(IsolateThread thread, CCharPointer model) {
        String a;
        try {
            String m = text(model);
            a = Folded.of(() -> Integer.toString(Boundary.holdModel(m)));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code Boundary.holdModelOver}: the handle that names {@code model} over {@code library}, the library compiled
     *  once per isolate. */
    @CEntryPoint(name = "lite_hold_model_over")
    static CCharPointer holdModelOver(IsolateThread thread, CCharPointer model, CCharPointer library) {
        String a;
        try {
            String m = text(model);
            String l = text(library);
            a = Folded.of(() -> Integer.toString(Boundary.holdModelOver(m, l)));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code Boundary.releaseModel}: the model let go of once. */
    @CEntryPoint(name = "lite_release_model")
    static CCharPointer releaseModel(IsolateThread thread, CCharPointer handle) {
        String a;
        try {
            String h = text(handle);
            a = Folded.of(() -> {
                Boundary.releaseModel(Integer.parseInt(h));
                return "";
            });
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }






    /** A host's own file read into its database, as a plan ({@code Boundary.fileLoad}, B3): the tab's
     *  {@code fileLoadOrError}, a DataCube planner's worker's {@code fileLoad}. */
    @CEntryPoint(name = "lite_file_load")
    static CCharPointer fileLoad(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.fileLoad(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** A remote file mounted, as a plan ({@code Boundary.remoteMount}, B3's Q3): the tab's {@code remoteMountOrError},
     *  a DataCube planner's worker's {@code remoteMount}. */
    @CEntryPoint(name = "lite_remote_mount")
    static CCharPointer remoteMount(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.remoteMount(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** A snapshot from a remote plane, as a plan ({@code Boundary.snapStream}, the user's ruling of 2026-11-11): the tab's
     *  {@code snapStreamOrError}, Python's warehouse snap -- the request the pull (model, runtime, lambda), planned in
     *  this call, never a statement (the user's ruling of 2026-11-12). */
    @CEntryPoint(name = "lite_snap_stream")
    static CCharPointer snapStream(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.snapStream(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** A snapshot's copy into the host's own database, as one plan ({@code Boundary.snapCopy}, the final audit's 1(b),
     *  2026-11-12): the tab's {@code snapCopyOrError}, Python's local copy -- the request the pull, planned in this call,
     *  never a statement. */
    @CEntryPoint(name = "lite_snap_copy")
    static CCharPointer snapCopy(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.snapCopy(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** A DataCube table Output as one plan ({@code Boundary.outputPlan}, the final audit's item 2, 2026-11-12): the tab's
     *  {@code outputPlanOrError}, Python's headless flow -- the table made before the write, one transaction. */
    @CEntryPoint(name = "lite_output_plan")
    static CCharPointer outputPlan(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.outputPlan(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** Rows the database generates into a declared table, as a plan ({@code Boundary.synthesize}: SYNTHESIZE, the user's
     *  ruling of 2026-11-12): the tab's {@code synthesizeOrError}, a headless host's. */
    @CEntryPoint(name = "lite_synthesize")
    static CCharPointer synthesize(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.synthesize(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** A host's value read as a relation, as a plan ({@code Boundary.frameLoad}, the user's ruling of 2026-11-11):
     *  Python's frames. */
    @CEntryPoint(name = "lite_frame_load")
    static CCharPointer frameLoad(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.frameLoad(r));
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

    /** A host's named operations on its own database of the given type ({@code Boundary.hostOperations}). */
    @CEntryPoint(name = "lite_host_operations")
    static CCharPointer hostOperations(IsolateThread thread, CCharPointer databaseType, CCharPointer operations) {
        String a;
        try {
            String d = text(databaseType), o = text(operations);
            a = Folded.of(() -> Boundary.hostOperations(d, o));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** A project's own database on a database of the given type ({@code Boundary.projectData}). */
    @CEntryPoint(name = "lite_project_data")
    static CCharPointer projectData(IsolateThread thread, CCharPointer model, CCharPointer databaseType) {
        String a;
        try {
            String m = text(model), d = text(databaseType);
            a = Folded.of(() -> Boundary.projectData(m, d));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    // legend-engine's pure/v1 call and execute's halves (lite_pure_v1, lite_execute_plan, lite_execute_json_plan,
    // lite_execute_json_answer, lite_execute_arrow_answer) are the edge's entry points: com.legend.edge.nativelib.
    // EngineEntries, native:edge_entry (F's separation census line A1, 2026-11-17)

    /** The planner's boot layer built for a model now ({@code Boundary.warmModel}; the protocol's {@code warm}, C2). */
    @CEntryPoint(name = "lite_warm_model")
    static CCharPointer warmModel(IsolateThread thread, CCharPointer model) {
        String a;
        try {
            String m = text(model);
            a = Folded.of(() -> {
                Boundary.warmModel(m);
                return "";
            });
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /**
     * The media types of legend-lite's sites by extension, as one JSON object ({@code com.legend.site.SiteTypes}): the
     * one table the warehouse's server answers with, which Python's engine serves its site by
     * (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C4).
     */
    @CEntryPoint(name = "lite_site_types")
    static CCharPointer siteTypes(IsolateThread thread) {
        String a;
        try {
            a = Folded.of(com.legend.site.SiteTypes::json);
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    // ---- lite's own grammar and typing, for Python's product (the edge program's H4, 2026-11-17): the four Boundary
    // ---- operations the tab exports (its A1), asked here instead of legend-engine's pure/v1 routes, each answer
    // ---- folded with lite's own refusal kinds (Folded.of), never legend-engine's errorType.









    /** LITE'S REFUSAL KINDS by name ({@code Boundary.refusalKinds}: {@code com.legend.lite.LiteWire.Kind}'s names, the texts a
     *  refusal of each is folded with and the HTTP status it is answered with) -- so a host reads the kinds and their
     *  statuses from the product, never from a constant of its own (the lite API spec's §3, 2026-11-17; the statuses
     *  since H4's P2, 2026-11-19). */
    @CEntryPoint(name = "lite_refusal_kinds")
    static CCharPointer refusalKinds(IsolateThread thread) {
        String a;
        try {
            a = Folded.of(Boundary::refusalKinds);
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code query/run}'s request read as the server's route reads it ({@code Boundary.queryRunRequest}: H4's P4,
     *  2026-11-19): the request JSON in, {@code {model, query, runtime, format, params}} or its refusal out. */
    @CEntryPoint(name = "lite_query_run_request")
    static CCharPointer queryRunRequest(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.queryRunRequest(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code query/run}'s JSON answer from the rows Python's database answered the plan with ({@code
     *  Boundary.queryRunJson}: H4's P5, 2026-11-19): the plan and the rows in, the server's answer text out. */
    @CEntryPoint(name = "lite_query_run_json")
    static CCharPointer queryRunJson(IsolateThread thread, CCharPointer plan, CCharPointer rows) {
        String a;
        try {
            String p = text(plan), r = text(rows);
            a = Folded.of(() -> Boundary.queryRunJson(p, r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code query/run}'s Arrow facts for the plan Python's database ran ({@code Boundary.queryRunArrowFacts}: H4's P5,
     *  2026-11-19): the plan in, the stream's and each field's metadata out. */
    @CEntryPoint(name = "lite_query_run_arrow_facts")
    static CCharPointer queryRunArrowFacts(IsolateThread thread, CCharPointer plan) {
        String a;
        try {
            String p = text(plan);
            a = Folded.of(() -> Boundary.queryRunArrowFacts(p));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** Lite's routes as Python's server answers them ({@code Boundary.liteRoute}: H4's P4, 2026-11-19): a call's method,
     *  path, body and the host's user in, its status, media type and body out ({@code Folded.http}). */
    @CEntryPoint(name = "lite_route")
    static CCharPointer liteRoute(IsolateThread thread, CCharPointer method, CCharPointer path, CCharPointer body,
            CCharPointer user) {
        String a;
        try {
            String m = text(method), p = text(path), b = text(body), u = text(user);
            a = Folded.http(() -> Boundary.liteRoute(m, p, b, u));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    // ---- LITE'S OWN API, v1 (docs/records/lite-api-v1-2026-11-17.md §2, §5, §7; the host train, 2026-11-17): one
    // ---- native entry per operation, named by the spec's one rule (operation a/b -> lite_a_b), each the operation's
    // ---- request JSON in and its answer out, over the product Boundary member of the same operation. No query/run:
    // ---- Python runs a plan on its own database (query/plan here, then its runner).
    /** {@code model/check}: the operation's request JSON in, its answer out ({@code Boundary.modelCheck}). */
    @CEntryPoint(name = "lite_model_check")
    static CCharPointer modelCheck(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.modelCheck(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code model/elements}: the operation's request JSON in, its answer out ({@code Boundary.modelElements}). */
    @CEntryPoint(name = "lite_model_elements")
    static CCharPointer modelElementsEntry(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.modelElements(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code query/parse}: the operation's request JSON in, its answer out ({@code Boundary.queryParse}). */
    @CEntryPoint(name = "lite_query_parse")
    static CCharPointer queryParse(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.queryParse(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code query/print}: the operation's request JSON in, its answer out ({@code Boundary.queryPrint}). */
    @CEntryPoint(name = "lite_query_print")
    static CCharPointer queryPrint(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.queryPrint(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code query/type}: the operation's request JSON in, its answer out ({@code Boundary.queryType}). */
    @CEntryPoint(name = "lite_query_type")
    static CCharPointer queryType(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.queryType(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code query/plan}: the operation's request JSON in, its answer out ({@code Boundary.queryPlan}). */
    @CEntryPoint(name = "lite_query_plan")
    static CCharPointer queryPlan(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.queryPlan(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code model/createClass}: the operation's request JSON in, its answer out ({@code Boundary.modelCreateClass}). */
    @CEntryPoint(name = "lite_model_create_class")
    static CCharPointer modelCreateClass(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.modelCreateClass(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code model/createAssociation}: the operation's request JSON in, its answer out ({@code Boundary.modelCreateAssociation}). */
    @CEntryPoint(name = "lite_model_create_association")
    static CCharPointer modelCreateAssociation(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.modelCreateAssociation(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code model/createDataSpace}: the operation's request JSON in, its answer out ({@code Boundary.modelCreateDataSpace}). */
    @CEntryPoint(name = "lite_model_create_data_space")
    static CCharPointer modelCreateDataSpace(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.modelCreateDataSpace(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code ts/statement}: the operation's request JSON in, its answer out ({@code Boundary.tsStatement}). */
    @CEntryPoint(name = "lite_ts_statement")
    static CCharPointer tsStatement(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.tsStatement(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code ts/item}: the operation's request JSON in, its answer out ({@code Boundary.tsItem}). */
    @CEntryPoint(name = "lite_ts_item")
    static CCharPointer tsItem(IsolateThread thread, CCharPointer request) {
        String a;
        try {
            String r = text(request);
            a = Folded.of(() -> Boundary.tsItem(r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    // ---- a held model (the binding's cache, the spec's §7a): query/plan and query/type over the model a handle holds

    /** {@code query/plan} over a held model: {@code {query, runtime}} in, {@code {plan}} out ({@code Boundary.queryPlanHeld}). */
    @CEntryPoint(name = "lite_query_plan_held")
    static CCharPointer queryPlanHeld(IsolateThread thread, CCharPointer handle, CCharPointer request) {
        String a;
        try {
            int h = Integer.parseInt(text(handle));
            String r = text(request);
            a = Folded.of(() -> Boundary.queryPlanHeld(h, r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return answer(a);
    }

    /** {@code query/type} over a held model: {@code {query}} in, {@code {columns}} out ({@code Boundary.queryTypeHeld}). */
    @CEntryPoint(name = "lite_query_type_held")
    static CCharPointer queryTypeHeld(IsolateThread thread, CCharPointer handle, CCharPointer request) {
        String a;
        try {
            int h = Integer.parseInt(text(handle));
            String r = text(request);
            a = Folded.of(() -> Boundary.queryTypeHeld(h, r));
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
