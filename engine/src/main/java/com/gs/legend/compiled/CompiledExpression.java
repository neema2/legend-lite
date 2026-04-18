package com.gs.legend.compiled;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.compiler.TypeInfo;

import java.util.Map;

/**
 * Reusable primitive for every compiled body in the system.
 *
 * <p>Stores a resolved {@link ValueSpecification} AST together with its
 * per-node {@link TypeInfo} side table and first-class dependency data.
 * Produced by {@code TypeChecker.check(ValueSpecification)} and stored on
 * {@code CompiledFunction.body}, {@code CompiledService.queryBody},
 * {@code CompiledDerivedProperty.body}, {@code CompiledConstraint.body},
 * and {@code CompiledMappedClass.sourceSpec}.
 *
 * <p><strong>Node-ID note:</strong> in memory, {@code types} is an
 * {@code IdentityHashMap<ValueSpecification, TypeInfo>} — mechanically
 * identical to today's {@code TypeCheckResult}. Persistence (Phase 7) will
 * introduce a parallel stable-node-ID encoding; the in-memory shape is
 * unaffected.
 *
 * <p>This record <strong>replaces {@code TypeCheckResult}</strong> as the
 * public typed-body contract.
 *
 * @param ast          Top-level AST node for this body.
 * @param types        Per-node type info side table (identity-keyed).
 * @param dependencies Member-level dependency data (class property accesses
 *                     and association navigations) observed during compilation.
 */
public record CompiledExpression(
        ValueSpecification ast,
        Map<ValueSpecification, TypeInfo> types,
        CompiledDependencies dependencies) {

    /**
     * Looks up the {@link TypeInfo} for a specific AST node. Throws if not
     * stamped — that's a compiler bug (see {@code AGENTS.md} invariant #1).
     */
    public TypeInfo typeInfoFor(ValueSpecification node) {
        TypeInfo info = types.get(node);
        if (info == null) {
            throw new PureCompileException(
                    "PlanGenerator: no TypeInfo for " + node.getClass().getSimpleName()
                            + " — TypeChecker must stamp every node that PlanGenerator reads");
        }
        return info;
    }
}
