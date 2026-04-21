package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.model.m3.Type;

/**
 * Type cast / coercion. Covers {@code cast(expr, @Type)}, {@code toOne},
 * {@code toMany}, and similar multiplicity/type narrowing operators.
 *
 * @param expr       The value being cast.
 * @param targetType The Pure type being cast to (null if it's a pure multiplicity coerce).
 * @param info       Result type + multiplicity after the cast.
 */
public record TypedCast(
        TypedSpec expr,
        Type targetType,
        ExpressionType info
) implements TypedSpec {}
