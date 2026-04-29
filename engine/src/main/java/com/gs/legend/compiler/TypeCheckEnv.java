package com.gs.legend.compiler;

import com.gs.legend.ast.LambdaFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiler.typed.TypedSpec;

import java.util.Optional;

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
     * <p>Strict variant — used by the build path ({@code compileMapping}),
     * where we are compiling a declared {@link com.gs.legend.model.m3.MappingDefinition}
     * and a missing function FQN means the {@code MappingNormalizer} produced
     * inconsistent state.
     *
     * @throws PureCompileException if the class has no mapping in the
     *         active scope.
     */
    CompiledFunction compileMappingFunctionFor(String classFqn);

    /**
     * Probe variant of {@link #compileMappingFunctionFor(String)} — returns
     * empty when the class has no mapping in the active scope, instead of
     * throwing.
     *
     * <p>Used by the query path ({@code GetAllChecker}, pass-2
     * association-target fan-out) where missing mapping is a back-end /
     * link-time error, not a type error: a query may reference a class
     * for typing purposes whose mapping isn't realized in the current
     * runtime. The eventual error fires precisely at the back-end use
     * site (e.g. {@code SourceLowering}) rather than masking the typing
     * result.
     *
     * <p>This mirrors how a real toolchain separates type-check (front-end)
     * from link/codegen (back-end): the type of {@code Class.all()} is
     * {@code Class[*]} regardless of whether the mapping is present.
     */
    Optional<CompiledFunction> tryCompileMappingFunctionFor(String classFqn);

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

    /**
     * Generate a fresh synthetic identifier — a monotonic gensym, scoped to this
     * {@link TypeChecker} instance (i.e. per compilation cycle).
     *
     * <p>Checkers that need to introduce a binding for an intermediate value
     * (e.g. {@code ExtendChecker} naming the result of a window call inside an
     * outer wrapper) call this to obtain a name that cannot collide with any
     * user-written Pure identifier.
     *
     * <p>Uniqueness scope: sufficient for ANF-style let-insertion where the
     * binding is created and consumed inside a single HIR sub-tree. The
     * {@code $$} convention guarantees non-collision with user names; the
     * counter guarantees non-collision between sibling inserted bindings.
     *
     * @param prefix  identifier prefix, typically starting with the reserved
     *                {@code $$} marker (e.g. {@code "$$wh"}).
     * @return  {@code prefix} concatenated with a fresh monotonic integer.
     */
    String freshSymbol(String prefix);
}
