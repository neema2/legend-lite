package com.gs.legend.compiler.typed;

import java.util.List;

/** Traverse extend: association-chain projection into the source relation. */
public record TypedTraverseExtendCol(
        String alias,
        List<TraversalHop> hops,
        TypedLambda expression
) implements TypedExtendCol {
    public TypedTraverseExtendCol {
        hops = List.copyOf(hops);
    }
}
