package com.gs.legend.compiled;

import com.gs.legend.compiler.typed.TypedSpec;

/**
 * Reusable primitive for every compiled body in the system.
 *
 * <p>Post-bigbang the compiled body is a pure <strong>typed HIR</strong> —
 * a tree of {@link TypedSpec} nodes that carry type, multiplicity, and all
 * resolved metadata directly on each variant. There is no AST sidecar, no
 * identity-keyed map; downstream passes (MappingResolver, PlanGenerator)
 * traverse the HIR via structural pattern matching.
 *
 * @param hir          Typed HIR root for this compiled body.
 * @param dependencies Member-level dependency data (class property accesses
 *                     and association navigations observed during compilation).
 */
public record CompiledExpression(
        TypedSpec hir,
        CompiledDependencies dependencies) {
}
