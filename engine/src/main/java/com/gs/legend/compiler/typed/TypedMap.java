package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * {@code source->map(mapper)} — covers both explicit {@code map} and
 * autoMap (lifting of {@code $x.prop} over collections).
 */
public record TypedMap(
        TypedSpec source,
        TypedLambda mapper,
        ExpressionType info
) implements TypedSpec {}
