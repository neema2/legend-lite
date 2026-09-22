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
public record TypedGraphTree(String property, List<TypedGraphTree> children,
        @com.legend.base.Nullable String alias, List<TypedSpec> args, boolean sweep,
        @com.legend.base.Nullable String subTypeFqn,
        boolean qualified) {
    public TypedGraphTree {
        Objects.requireNonNull(property, "property");
        children = List.copyOf(children);
        args = args == null ? List.of() : List.copyOf(args);
    }

    /** Historical arity: parenthesized-ness follows the args. */
    public TypedGraphTree(String property, List<TypedGraphTree> children,
            @com.legend.base.Nullable String alias, List<TypedSpec> args, boolean sweep,
            @com.legend.base.Nullable String subTypeFqn) {
        this(property, children, alias, args, sweep, subTypeFqn,
                args != null && !args.isEmpty());
    }

    /** Non-subType node (every pre-subType shape). */
    public TypedGraphTree(String property, List<TypedGraphTree> children,
            @com.legend.base.Nullable String alias, List<TypedSpec> args, boolean sweep) {
        this(property, children, alias, args, sweep, null);
    }

    /** Non-sweep node with alias and call args (the pre-sweep shape). */
    public TypedGraphTree(String property, List<TypedGraphTree> children,
            @com.legend.base.Nullable String alias, List<TypedSpec> args) {
        this(property, children, alias, args, false, null);
    }

    /** Aliased node without call args. */
    public TypedGraphTree(String property, List<TypedGraphTree> children,
            @com.legend.base.Nullable String alias) {
        this(property, children, alias, List.of(), false, null);
    }

    /** Un-aliased node (the common spelling). */
    public TypedGraphTree(String property, List<TypedGraphTree> children) {
        this(property, children, null, List.of(), false, null);
    }
}
