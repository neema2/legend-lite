package com.gs.legend.compiler.typed;

import java.util.List;
import java.util.Optional;

/**
 * Projection column for {@link TypedProject}.
 *
 * <p>{@code associationPath} carries the walked navigation (e.g., {@code [address, city]})
 * when the projection traverses through associations; empty/absent for local projections.
 */
public record TypedProjectionCol(
        String alias,
        TypedLambda expression,
        Optional<List<String>> associationPath
) {}
