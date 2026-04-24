package com.gs.legend.compiled;

import com.gs.legend.compiler.ResolvedMappings;

/**
 * MappingResolver's output — a {@link CompiledExpression} plus its committed
 * {@link ResolvedMappings} sidecar.
 *
 * <p>Phase ordering is expressed in the type system: MappingResolver consumes
 * a {@link CompiledExpression} and produces a {@code ResolvedExpression};
 * PlanGenerator consumes a {@code ResolvedExpression} and will not compile
 * with a weaker argument. There is no "did MR run?" runtime check — the
 * compiler refuses the wrong ordering.
 *
 * @param compiled TypeChecker's output: typed HIR + dependencies.
 * @param mappings MappingResolver's output: per-node store resolutions,
 *                 navigations, and access bindings.
 */
public record ResolvedExpression(
        CompiledExpression compiled,
        ResolvedMappings mappings) {

    /** @return the typed HIR root, threading through the inner CompiledExpression. */
    public com.gs.legend.compiler.typed.TypedSpec hir() { return compiled.hir(); }

    /** @return the compiled dependencies, threading through the inner CompiledExpression. */
    public CompiledDependencies dependencies() { return compiled.dependencies(); }
}
