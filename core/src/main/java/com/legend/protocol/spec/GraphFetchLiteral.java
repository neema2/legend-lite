package com.legend.protocol.spec;

import java.util.List;
import java.util.Objects;

/**
 * A graph-fetch literal {@code #{Root {a, k {b}}}#} — like {@link PathLiteral}, the parse
 * product keeps BOTH representations: the wire tree (class name + property nodes with their
 * REAL token spans — graph-fetch spans are absolute, not island-shifted) for
 * {@code ProtocolEmitter}, and the desugared {@link ColSpecArray} legend-lite's compiler
 * consumes. {@code NameResolver} dissolves the node into {@link #desugared()} on first
 * touch.
 *
 * <p>Wire shape (ProbeWireShapes "typed new and gft", "alias dated tref2 gft2" d):
 * {@code classInstance} of type {@code rootGraphFetchTree}; the OUTER span and the value's
 * span are both the CLASS-NAME token span; each property node spans its name token.
 * Aliases, parameters, and subtype trees are carried as an unsupported flag and wall.
 */
public record GraphFetchLiteral(
        String className,
        List<Node> subTrees,
        List<SubTypeNode> subTypeTrees,
        ValueSpecification desugared,
        boolean unsupported,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) implements ValueSpecification {

    public GraphFetchLiteral {
        Objects.requireNonNull(className, "className");
        Objects.requireNonNull(subTrees, "subTrees");
        Objects.requireNonNull(subTypeTrees, "subTypeTrees");
        Objects.requireNonNull(desugared, "desugared");
        subTrees = List.copyOf(subTrees);
        subTypeTrees = List.copyOf(subTypeTrees);
    }

    /** No-subtype convenience constructor. */
    public GraphFetchLiteral(String className, List<Node> subTrees,
            ValueSpecification desugared, boolean unsupported,
            @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) {
        this(className, subTrees, List.of(), desugared, unsupported, pos);
    }

    /** A {@code ->subType(@X) { ... }} ENTRY — the level's subTypeTrees on the wire;
     *  {@code pos} is the class-name span WITHOUT the {@code @}. */
    public record SubTypeNode(String subTypeClass,
                              @com.legend.base.Nullable com.legend.protocol.SourceInfo pos,
                              List<Node> subTrees) {
        public SubTypeNode {
            subTrees = List.copyOf(subTrees);
        }
    }

    /**
     * One property node: name, its token span, call arguments, optional alias
     * ({@code 'nick' : prop}), optional subtype view ({@code prop->subType(@X)}), nested
     * subtrees. Arguments are protocol value specs whose spans the island scan bakes in
     * (var = name only, no {@code $}; string/date/enum = full literal).
     */
    public record Node(String property,
                       @com.legend.base.Nullable com.legend.protocol.SourceInfo pos,
                       List<ValueSpecification> parameters,
                       @com.legend.base.Nullable String alias,
                       @com.legend.base.Nullable String subType,
                       List<Node> subTrees,
                       List<SubTypeNode> subTypeTrees) {
        public Node {
            parameters = List.copyOf(parameters);
            subTrees = List.copyOf(subTrees);
            subTypeTrees = List.copyOf(subTypeTrees);
        }

        /** No-subtype-entries convenience constructor. */
        public Node(String property, @com.legend.base.Nullable com.legend.protocol.SourceInfo pos,
                    List<ValueSpecification> parameters, @com.legend.base.Nullable String alias,
                    @com.legend.base.Nullable String subType, List<Node> subTrees) {
            this(property, pos, parameters, alias, subType, subTrees, List.of());
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof GraphFetchLiteral other
                && className.equals(other.className())
                && desugared.equals(other.desugared())
                && unsupported == other.unsupported();
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, desugared, unsupported);
    }
}
