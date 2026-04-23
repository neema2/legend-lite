package com.gs.legend.compiler.typed;

import java.util.List;

/**
 * Association extend: association property exposed on a relational source as a
 * typed join chain. Produced by {@link com.gs.legend.compiler.checkers.ExtendChecker}
 * for synthesized sourceSpec extend clauses of the form
 * {@code ~propName:{-> traverse(targetTable, {prev,hop|cond})}} where the 0-param
 * outer lambda and the inner traverse chain are first-class, not a raw-AST pattern.
 *
 * <p>Unlike {@link TypedScalarExtendCol}, association extends do not add a column
 * to the source schema — they declare a navigable join that {@code MappingResolver}
 * lowers to a {@link com.gs.legend.compiler.StoreResolution.JoinResolution}, and
 * {@code PlanGenerator} lowers to a correlated subquery / graphFetch projection.
 *
 * <p>The target class's compiled mapping function is <em>not</em> attached here.
 * It is compiled in a pass-2 fan-out over
 * {@link com.gs.legend.compiled.CompiledDependencies#associationNavigations}
 * and exposed via
 * {@link com.gs.legend.compiled.CompiledDependencies#mappingFunctions}, which
 * {@code MappingResolver} consults by target class FQN. This keeps the node
 * immutable and avoids an owner-class context threading through
 * {@code CompilationContext}.
 *
 * @param alias Target association property name on the owning class.
 * @param hops  Ordered join hops from the source row to the target table.
 */
public record TypedAssociationExtendCol(
        String alias,
        List<TraversalHop> hops
) implements TypedExtendCol {}
