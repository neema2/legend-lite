package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;

/**
 * Callback interface for individual type-checkers.
 *
 * <p>
 * Individual function checkers (FilterChecker, SortChecker, etc.) receive a
 * {@code TypeCheckEnv} to compile sub-expressions and access shared services.
 * TypeChecker implements this interface.
 */
public interface TypeCheckEnv {

    /** Compile a value specification and return its type info. */
    TypeInfo compileExpr(ValueSpecification vs, TypeChecker.CompilationContext ctx);

    /** Access model context for class hierarchy resolution (e.g., LCA in concatenate). */
    com.gs.legend.model.ModelContext modelContext();

    /** Look up the TypeInfo for a previously compiled AST node (by identity). */
    TypeInfo lookupCompiled(ValueSpecification vs);

    /**
     * Compile a class's sourceSpec (relational or M2M) once, idempotently.
     * Single primitive shared by the query path (GetAllChecker, pass-2
     * association targets) and the build path (compileMapping fan-out).
     */
    void compileSourceSpecFor(String classFqn);
}
