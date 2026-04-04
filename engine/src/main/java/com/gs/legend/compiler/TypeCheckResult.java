package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Output of {@link TypeChecker}: bundles the top-level AST
 * with the per-node side table.
 *
 * <p>
 * This is the single artifact that flows from compilation to plan generation.
 * The plan generator ({@code PlanGenerator}) consumes this — no model context
 * or mapping parameter threading needed.
 *
 * @param root                   Top-level AST node (raw ValueSpecification, no wrapper)
 * @param types                  Per-node type info side table (node → TypeInfo)
 * @param classPropertyAccesses  Class property accesses observed during compilation (className → property names)
 */
public record TypeCheckResult(
        ValueSpecification root,
        IdentityHashMap<ValueSpecification, TypeInfo> types,
        Map<String, Set<String>> classPropertyAccesses) {

    /** Looks up the TypeInfo for a specific AST node. Throws if not stamped — that's a compiler bug. */
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

