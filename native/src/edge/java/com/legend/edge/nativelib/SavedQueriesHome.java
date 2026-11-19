package com.legend.edge.nativelib;

import com.legend.nativelib.Compiler;
import com.legend.queries.FileCubeRecords;
import com.legend.queries.FileRecords;
import com.legend.queries.SavedCubes;
import com.legend.queries.SavedQueries;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import planner.Folded;

import java.nio.file.Path;

/**
 * PYTHON'S SAVED QUERIES (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B5), AT THE EDGE (the separation census's A2,
 * 2026-11-17: they answer upstream's /api/pure/v1/query, so legend-engine's words; the native image carries the edge as
 * the server does, the orchestrator's extension of ruling A4): legend-engine's query store as legend-lite's
 * rules answer it ({@link SavedQueries}, the very rules its server runs on the JVM and the page runs compiled to
 * WebAssembly), over a folder in this user's legend-lite folder, one JSON file per query ({@link FileRecords}, the
 * server's layout). Each call is one request -- {@code rest} the path after {@code /api/pure/v1/query}, {@code query}
 * its raw query string (empty for none) -- as {@code user}, answered {@code "OK\n<status>\n<body>"} as
 * {@code ModelHome}'s are. The caller serializes the calls on a folder, across processes too (Python holds a lock around
 * each); nothing is kept in memory between calls. DataCube's saved pages are answered the same way ({@link SavedCubes},
 * B6), over Python's {@code ll.home()/pages}.
 */
final class SavedQueriesHome {
    private SavedQueriesHome() {
    }

    @CEntryPoint(name = "lite_saved_queries")
    static CCharPointer savedQueries(IsolateThread thread, CCharPointer folder, CCharPointer method, CCharPointer rest,
            CCharPointer query, CCharPointer body, CCharPointer user) {
        String a;
        try {
            String f = Compiler.text(folder), m = Compiler.text(method), r = Compiler.text(rest), q = Compiler.text(query),
                    b = Compiler.text(body), u = Compiler.text(user);
            a = Folded.of(() -> {
                SavedQueries.Answer answer = SavedQueries.answer(new SavedQueries(new FileRecords(Path.of(f)),
                        System::currentTimeMillis), m, r, q.isEmpty() ? null : q, b, u,
                        // upstream's /api/pure/v1/query, so legend-engine's words (the stage-(a) red, 2026-11-16: W8 left
                        // this host on lite's; the page's and the server's mount word it so)
                        com.legend.edge.StoreWords::of);
                // and in legend-engine's shape (J9 of the edge audit, 2026-11-17: the product's store answers {kind,
                // message}; legend-engine's {message} and its envelope are the edge's)
                return answer.status() + "\n" + com.legend.edge.StoreWords.engineShape(answer.status(), answer.json());
            });
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return Compiler.answer(a);
    }

    /** One {@code /api/pure/v1/query/dataCube...} request over DataCube's saved pages in {@code folder} (B6: Python's
     *  {@code ll.home()/pages}, the layout its pages were kept in), answered as {@link #savedQueries}'. */
    @CEntryPoint(name = "lite_saved_cubes")
    static CCharPointer savedCubes(IsolateThread thread, CCharPointer folder, CCharPointer method, CCharPointer rest,
            CCharPointer query, CCharPointer body, CCharPointer user) {
        String a;
        try {
            String f = Compiler.text(folder), m = Compiler.text(method), r = Compiler.text(rest), q = Compiler.text(query),
                    b = Compiler.text(body), u = Compiler.text(user);
            a = Folded.of(() -> {
                SavedQueries.Answer answer = SavedCubes.answer(new SavedCubes(new FileCubeRecords(Path.of(f)),
                        System::currentTimeMillis), m, r, q.isEmpty() ? null : q, b, u,
                        // upstream's /api/pure/v1/query, so legend-engine's words (the stage-(a) red, 2026-11-16: W8 left
                        // this host on lite's; the page's and the server's mount word it so)
                        com.legend.edge.StoreWords::of);
                // and in legend-engine's shape (J9 of the edge audit, 2026-11-17: the product's store answers {kind,
                // message}; legend-engine's {message} and its envelope are the edge's)
                return answer.status() + "\n" + com.legend.edge.StoreWords.engineShape(answer.status(), answer.json());
            });
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return Compiler.answer(a);
    }
}
