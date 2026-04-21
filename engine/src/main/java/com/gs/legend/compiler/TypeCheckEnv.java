package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.typed.TypedSpec;

/**
 * Callback interface for individual type-checkers.
 *
 * <p>Individual function checkers (FilterChecker, SortChecker, etc.) receive a
 * {@code TypeCheckEnv} to compile sub-expressions and access shared services.
 * TypeChecker implements this interface.
 *
 * <p>Post-bigbang shape: compile returns {@link TypedSpec}. All type and
 * analysis data lives embedded inside each typed node; no external sidecar.
 */
public interface TypeCheckEnv {

    /** Compile a value specification into its typed HIR form. */
    TypedSpec compileExpr(ValueSpecification vs, TypeChecker.CompilationContext ctx);

    /** Access model context for class hierarchy resolution (e.g., LCA in concatenate). */
    com.gs.legend.model.ModelContext modelContext();

    /**
     * Compile a class's sourceSpec (relational or M2M) once, idempotently.
     * Single primitive shared by the query path (GetAllChecker, pass-2
     * association targets) and the build path (compileMapping fan-out).
     */
    void compileSourceSpecFor(String classFqn);
}
