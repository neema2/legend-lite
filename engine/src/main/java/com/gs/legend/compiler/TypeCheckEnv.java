package com.gs.legend.compiler;

import com.gs.legend.ast.LambdaFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiled.CompiledFunction;
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
     * Compile the synthetic mapping function for a class to a
     * {@link CompiledFunction}, once per FQN (idempotent / memoized).
     *
     * <p>Single primitive shared by the query path ({@code GetAllChecker},
     * pass-2 association-target fan-out) and the build path
     * ({@code compileMapping}). Returns the compiled function so callers
     * can attach it to typed HIR nodes (e.g. {@code TypedGetAll.mappingFn})
     * without re-resolving downstream.
     *
     * @throws PureCompileException if the class has no mapping in the
     *         active scope — the anchor is a hard contract, not a probe.
     */
    CompiledFunction compileMappingFunctionFor(String classFqn);

    /**
     * Compile a lambda body as a statement list with let-chaining:
     * intermediate {@code letFunction} statements emit {@link com.gs.legend.compiler.typed.TypedLet}
     * and bind into the context's {@code letBindings}, the terminal statement's
     * {@link TypedSpec} becomes the body's result, and multi-statement bodies
     * are wrapped in a {@link com.gs.legend.compiler.typed.TypedBlock}.
     *
     * <p>Single source of truth for both user-function bodies (routed through
     * the body-compile primitive) and built-in lambda arguments; guarantees
     * identical let-binding semantics regardless of whether the lambda is
     * passed to {@code map(..)}, {@code filter(..)}, or a user-declared function.
     */
    TypedSpec compileLambdaBody(LambdaFunction lambda, TypeChecker.CompilationContext ctx);
}
