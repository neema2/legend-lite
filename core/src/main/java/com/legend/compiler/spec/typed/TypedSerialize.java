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
 * @param engineExecuteCardinality COMPATIBILITY ONLY: this serialize is the
 *               query of a legend-engine {@code execute(...)} whose
 *               {@code .values} a statement reads, so its result follows the
 *               engine JsonBuilder's cardinality rule (exactly one object
 *               serializes bare, zero or several as the array). Set by the
 *               result-envelope splice alone ({@code ResultEnvelopeSplice});
 *               the resolver carries it to the {@link TypedSerializeGraph} it
 *               builds. Lite's own serialize results stay arrays (the user's
 *               ruling, 2026-11-03: "the right clean thing in lite, the
 *               backward-compatible thing in compatibility mode")
 */
public record TypedSerialize(TypedSpec source, List<TypedGraphTree> tree,
                             Optional<TypedSpec> config, ExprType info,
                             boolean engineExecuteCardinality) implements TypedSpec {
    public TypedSerialize {
        tree = List.copyOf(tree);
    }

    /** Every construction but the splice's: lite's own serialize. */
    public TypedSerialize(TypedSpec source, List<TypedGraphTree> tree,
            Optional<TypedSpec> config, ExprType info) {
        this(source, tree, config, info, false);
    }

    /** This serialize as the query of an engine {@code execute(...)} whose
     * values a statement reads (see {@link #engineExecuteCardinality}): the
     * result-envelope splice's alone; every other component verbatim. */
    public TypedSerialize withEngineExecuteCardinality() {
        return engineExecuteCardinality ? this
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
                        : java.util.Optional.empty(), info, engineExecuteCardinality);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedSerialize(source, tree, config, info, engineExecuteCardinality);
    }
}
