package com.legend.compiler.spec.typed;

import java.util.List;
import java.util.Objects;

/**
 * One node of a checked graph-fetch tree {@code #{Class{a, b{c}}}#}: a property
 * (validated against its owner class) and its sub-tree. A component of
 * {@link TypedGraphFetch} / {@link TypedSerialize} &mdash; not a {@link TypedSpec}.
 *
 * @param property the property name at this level
 * @param children the nested sub-tree, empty for a leaf
 */
public record TypedGraphTree(String property, List<TypedGraphTree> children) {
    public TypedGraphTree {
        Objects.requireNonNull(property, "property");
        children = List.copyOf(children);
    }
}
