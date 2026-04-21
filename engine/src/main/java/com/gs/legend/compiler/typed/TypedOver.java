package com.gs.legend.compiler.typed;

import java.util.List;
import java.util.Optional;

/** Window specification: PARTITION BY, ORDER BY, and optional frame. */
public record TypedOver(
        List<String> partitionBy,
        List<TypedSortKey> orderBy,
        Optional<TypedFrame> frame
) {
    public TypedOver {
        partitionBy = List.copyOf(partitionBy);
        orderBy = List.copyOf(orderBy);
    }
}
