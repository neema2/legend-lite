// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.nativelib;

import java.nio.file.Path;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;

import com.legend.queries.FileCubeRecords;
import com.legend.queries.FileRecords;
import com.legend.queries.SavedCubes;
import com.legend.queries.SavedQueries;

import planner.Folded;

/**
 * PYTHON'S SAVED QUERIES AND SAVED PAGES, THE PRODUCT'S (the edge program's H6, 2026-11-17): lite's own store at lite's
 * own mount ({@code /api/lite/v1/query}, as the server mounts it), refused in LITE'S words ({@link SavedQueries}'s and
 * {@link SavedCubes}'s own answers, never legend-engine's: those are the edge's, {@code edge.nativelib.SavedQueriesHome}
 * at upstream's {@code /api/pure/v1/query}). The same rules and the same folder layout as that entry: one JSON file per
 * query ({@link FileRecords}) and per page ({@link FileCubeRecords}), each call one request -- {@code rest} the path
 * after the store's mount, {@code query} its raw query string (empty for none) -- as {@code user}, answered
 * {@code "OK\n<status>\n<body>"}. The caller serializes the calls on a folder, across processes too (Python holds a lock
 * around each); nothing is kept in memory between calls.
 */
final class SavedStores {
    private SavedStores() {
    }

    /** One request of the saved queries in {@code folder}, at lite's mount, refused in lite's words. */
    @CEntryPoint(name = "lite_store_queries")
    static CCharPointer queries(IsolateThread thread, CCharPointer folder, CCharPointer method, CCharPointer rest,
            CCharPointer query, CCharPointer body, CCharPointer user) {
        String a;
        try {
            String f = Compiler.text(folder), m = Compiler.text(method), r = Compiler.text(rest), q = Compiler.text(query),
                    b = Compiler.text(body), u = Compiler.text(user);
            a = Folded.of(() -> {
                SavedQueries.Answer answer = SavedQueries.answer(new SavedQueries(new FileRecords(Path.of(f)),
                        System::currentTimeMillis), m, r, q.isEmpty() ? null : q, b, u);
                return answer.status() + "\n" + answer.json();
            });
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return Compiler.answer(a);
    }

    /** One request of DataCube's saved pages in {@code folder} ({@code ll.home()/pages}), at lite's mount, refused in
     *  lite's words. */
    @CEntryPoint(name = "lite_store_cubes")
    static CCharPointer cubes(IsolateThread thread, CCharPointer folder, CCharPointer method, CCharPointer rest,
            CCharPointer query, CCharPointer body, CCharPointer user) {
        String a;
        try {
            String f = Compiler.text(folder), m = Compiler.text(method), r = Compiler.text(rest), q = Compiler.text(query),
                    b = Compiler.text(body), u = Compiler.text(user);
            a = Folded.of(() -> {
                SavedQueries.Answer answer = SavedCubes.answer(new SavedCubes(new FileCubeRecords(Path.of(f)),
                        System::currentTimeMillis), m, r, q.isEmpty() ? null : q, b, u);
                return answer.status() + "\n" + answer.json();
            });
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return Compiler.answer(a);
    }
}
