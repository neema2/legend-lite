package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;
import java.util.Optional;

/**
 * Property access: {@code $x.name} or {@code $x.a.b.c} (multi-hop).
 *
 * <p>{@code associationPath} holds the walked navigation when the access traverses
 * through associations (e.g., {@code [address, city]} for {@code $p.address.city});
 * empty for direct single-hop access.
 */
public record TypedPropertyAccess(
        TypedSpec source,
        String property,
        Optional<List<String>> associationPath,
        ExpressionType info
) implements TypedSpec {}
