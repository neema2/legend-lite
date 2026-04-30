package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * {@code ClassName.all()} — the root relation for a mapped class.
 *
 * <p>The class's compiled mapping function body is <strong>not</strong>
 * attached on the node. It is compiled by TypeChecker's Pass-2 closure
 * (every class statically referenced by the body) and exposed via
 * {@code CompiledDependencies.mappingFunctions}, which lowering looks up
 * by class FQN through {@link com.gs.legend.plan.lowering.LoweringContext#compiledMappingFunction(String)}.
 *
 * <p>This mirrors {@link TypedAssociationExtendCol}'s shape: resolved
 * mapping refs live in a sidecar map keyed by FQN, not on individual
 * nodes — keeping the HIR immutable and avoiding two channels for the
 * same data.
 *
 * <p>Type-check is a front-end concern: the type of {@code Class.all()}
 * is {@code Class[*]} regardless of whether a mapping is registered.
 * Missing mapping is a back-end / link-time error surfaced at the
 * lowering use site ({@code SourceLowering}), not at type-check.
 *
 * @param className The class being materialized.
 * @param info      Type + multiplicity of the resulting relation.
 */
public record TypedGetAll(
        String className,
        ExpressionType info) implements TypedSpec {}
