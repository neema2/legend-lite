package com.gs.legend.compiler.typed;

/**
 * One hop in a traverse-extend chain.
 *
 * <p>TODO (Commit 5): define the concrete shape based on the existing
 * {@code Join} + {@code RelationalOperation} structure in
 * {@code MappingNormalizer.buildTraverseChain}. For Commit 1 this is a
 * placeholder so {@link TypedTraverseExtendCol} has a stable reference
 * shape. Filled in when TypeChecker lowering lands.
 */
public record TraversalHop() {}
