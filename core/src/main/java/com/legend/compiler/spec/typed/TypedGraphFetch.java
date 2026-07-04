package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A checked {@code graphFetch(#{Class{…}}#)} (engine {@code TypedGraphFetch})
 * &mdash; an object-graph projection: every tree property validates against its
 * owner class (recursively), and the result is the SOURCE type unchanged
 * (engine's rule &mdash; formatting is an execution concern).
 *
 * @param source the class collection being fetched
 * @param tree   the validated property tree
 * @param info   the source type unchanged
 */
public record TypedGraphFetch(TypedSpec source, List<TypedGraphTree> tree, ExprType info) implements TypedSpec {
    public TypedGraphFetch {
        tree = List.copyOf(tree);
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source);
    }
}
