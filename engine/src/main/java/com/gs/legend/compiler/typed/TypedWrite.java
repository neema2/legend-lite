package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Relational write/materialize: {@code source->write(destination)}. */
public record TypedWrite(
        TypedSpec source,
        TypedSpec destination,
        ExpressionType info
) implements TypedSpec {}
