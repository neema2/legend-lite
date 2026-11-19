package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;
import java.util.Optional;

/**
 * A checked {@code serialize(#{Class{…}}#)} (engine {@code SerializeChecker})
 * &mdash; serializes a class collection through a validated graph-fetch tree;
 * {@code String[1]} from the registered signature.
 *
 * @param source the class collection being serialized
 * @param tree   the validated property tree
 * @param config the optional serialization config argument
 * @param info   {@code String[1]}
 * @param readThroughExecuteValues what the program wrote: this serialize is the
 *               query of an {@code execute(...)} call whose {@code .values} a
 *               statement reads (train 17 item 1, the orchestrator's ruling of
 *               2026-11-18: a fact about the program, the product's, as
 *               {@code TypedTableReference#accessor}; until then a field named
 *               for legend-engine's rule). Set by the result-envelope splice
 *               alone; the resolver carries it to its graph. How such a root is
 *               presented is the plan style's ({@code LoweringForms#executeValues}):
 *               lite's array in the product, legend-engine's one-object fold at
 *               the edge.
 */
public record TypedSerialize(TypedSpec source, List<TypedGraphTree> tree,
                             Optional<TypedSpec> config, ExprType info,
                             boolean readThroughExecuteValues) implements TypedSpec {
    public TypedSerialize {
        tree = List.copyOf(tree);
    }

    /** Every construction but the splice's: lite's own serialize. */
    public TypedSerialize(TypedSpec source, List<TypedGraphTree> tree,
            Optional<TypedSpec> config, ExprType info) {
        this(source, tree, config, info, false);
    }

    /** This serialize as the query of an {@code execute(...)} call whose
     * values a statement reads (see {@link #readThroughExecuteValues}): the
     * result-envelope splice's alone; every other component verbatim. */
    public TypedSerialize readThroughExecute() {
        return readThroughExecuteValues ? this
                : new TypedSerialize(source, tree, config, info, true);
    }

    @Override
    public List<TypedSpec> children() {
        return config
                .map(c -> List.of(source, c))
                .orElseGet(() -> List.of(source));
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, config.isPresent() ? 2 : 1, "TypedSerialize");
        return new TypedSerialize(kids.get(0), tree,
                config.isPresent() ? java.util.Optional.of(kids.get(1))
                        : java.util.Optional.empty(), info, readThroughExecuteValues);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedSerialize(source, tree, config, info, readThroughExecuteValues);
    }
}
