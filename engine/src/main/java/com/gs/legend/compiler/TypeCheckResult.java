package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;

import java.util.IdentityHashMap;

/**
 * Output of {@link TypeChecker}: bundles the top-level AST
 * with the per-node side table.
 *
 * <p>
 * This is the single artifact that flows from compilation to plan generation.
 * The plan generator ({@code PlanGenerator}) consumes this — no model context
 * or mapping parameter threading needed.
 *
 * @param root  Top-level AST node (raw ValueSpecification, no wrapper)
 * @param types Per-node type info side table (node → TypeInfo)
 */
public record TypeCheckResult(
        ValueSpecification root,
        IdentityHashMap<ValueSpecification, TypeInfo> types) {

    /** Looks up the TypeInfo for a specific AST node. */
    public TypeInfo typeInfoFor(ValueSpecification node) {
        return types.get(node);
    }
}

