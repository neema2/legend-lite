package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * The {@code %latest} milestoning marker &mdash; {@code LatestDate[1]}.
 *
 * @param info {@code LatestDate[1]}
 */
public record TypedCLatestDate(ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
