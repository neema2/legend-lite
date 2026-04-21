package com.gs.legend.compiler.typed;

import java.util.List;

/** Group by an association-navigation path (e.g., {@code $p.address.city}). */
public record TypedAssociationGroupKey(List<String> path, String alias) implements TypedGroupKey {
    public TypedAssociationGroupKey {
        path = List.copyOf(path);
    }
}
