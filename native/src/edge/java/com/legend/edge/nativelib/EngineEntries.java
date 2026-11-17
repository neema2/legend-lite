// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge.nativelib;

import com.legend.edge.wasm.EdgeBoundary;
import com.legend.nativelib.Compiler;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import planner.Folded;

/**
 * PYTHON'S ENGINE ROUTES, AT THE EDGE (F's separation census line A1, 2026-11-17): legend-engine's {@code pure/v1} call
 * and execute's plan and answer halves, the C entry points the native library has always exported under these names,
 * answered through the edge ({@link EdgeBoundary}) as the tab's are. Moved from {@code nativelib.Compiler} unchanged;
 * the library is rooted at the edge as the server is (A2's {@code SavedQueriesHome}, the orchestrator's extension of
 * ruling A4).
 */
final class EngineEntries {
    private EngineEntries() {
    }

    /** One legend-engine pure/v1 call by its path and raw query, as legend-lite's server answers it ({@code Boundary.pureV1}). */
    @CEntryPoint(name = "lite_pure_v1")
    static CCharPointer pureV1(IsolateThread thread, CCharPointer path, CCharPointer rawQuery, CCharPointer body) {
        String a;
        try {
            String p = Compiler.text(path), q = Compiler.text(rawQuery), b = Compiler.text(body);
            a = Folded.http(() -> EdgeBoundary.pureV1(p, q, b));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return Compiler.answer(a);
    }

    /** Execute's plan half in upstream's Arrow format: the SQL and the schema metadata ({@code Boundary.executePlan}). */
    @CEntryPoint(name = "lite_execute_plan")
    static CCharPointer executePlan(IsolateThread thread, CCharPointer body, CCharPointer models) {
        String a;
        try {
            String b = Compiler.text(body), m = Compiler.text(models);
            a = Folded.http(() -> EdgeBoundary.executePlan(b, m));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return Compiler.answer(a);
    }

    /**
     * Execute's plan half in the engine's JSON, for a host that runs the SQL on its own database
     * ({@code Boundary.executeJsonPlan}; the planner protocol's {@code executePlan}, docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C2).
     */
    @CEntryPoint(name = "lite_execute_json_plan")
    static CCharPointer executeJsonPlan(IsolateThread thread, CCharPointer body, CCharPointer models) {
        String a;
        try {
            String b = Compiler.text(body), m = Compiler.text(models);
            a = Folded.http(() -> EdgeBoundary.executeJsonPlan(b, m));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return Compiler.answer(a);
    }

    /** That plan's answer from the rows the host's database answered ({@code Boundary.executeJsonAnswer}; C2). */
    @CEntryPoint(name = "lite_execute_json_answer")
    static CCharPointer executeJsonAnswer(IsolateThread thread, CCharPointer plan, CCharPointer rows) {
        String a;
        try {
            String p = Compiler.text(plan), r = Compiler.text(rows);
            a = Folded.http(() -> EdgeBoundary.executeJsonAnswer(p, r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return Compiler.answer(a);
    }

    /** That plan's answer in upstream's Arrow format from the rows the host's database answered
     *  ({@code Boundary.executeArrowAnswer}; track A, 2026-10-29): the zstd frame folded as base64. */
    @CEntryPoint(name = "lite_execute_arrow_answer")
    static CCharPointer executeArrowAnswer(IsolateThread thread, CCharPointer plan, CCharPointer rows) {
        String a;
        try {
            String p = Compiler.text(plan), r = Compiler.text(rows);
            a = Folded.http(() -> EdgeBoundary.executeArrowAnswer(p, r));
        } catch (Throwable failure) {
            a = Folded.failure(failure);
        }
        return Compiler.answer(a);
    }
}
