package com.gs.legend.compiler.typed;

import java.util.List;

/**
 * Reified graph-fetch property tree node, retained on
 * {@link TypedGraphFetch} and {@link TypedSerialize} so the planner can
 * shape the JSON envelope without re-walking the AST.
 *
 * <p>Each node is a single property: a name plus zero-or-more children.
 * Leaf scalars have an empty {@code children} list. A nested class hop
 * (e.g. {@code person { addresses { city } }}) is a node whose children
 * are the requested sub-properties.
 *
 * <p>Mirrors legend-engine's {@code RootGraphFetchTree} / {@code PropertyGraphFetchTree}
 * pair, simplified for legend-lite: there is no separate root vs. property
 * distinction at this level — the {@code TypedGraphFetch.children()} list
 * is the root's children.
 */
public record TypedGraphTree(
        String propertyName,
        List<TypedGraphTree> children
) {
    public TypedGraphTree {
        if (propertyName == null || propertyName.isBlank()) {
            throw new IllegalArgumentException("TypedGraphTree.propertyName must be non-blank");
        }
        children = children == null ? List.of() : List.copyOf(children);
    }

    /** Convenience: leaf scalar (no children). */
    public static TypedGraphTree leaf(String name) {
        return new TypedGraphTree(name, List.of());
    }

    public boolean isLeaf() { return children.isEmpty(); }
}
