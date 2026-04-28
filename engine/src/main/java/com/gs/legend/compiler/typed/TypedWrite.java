package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

/** Relational write/materialize: {@code source->write(destination)}. */
public record TypedWrite(
        TypedSpec source,
        TypedSpec destination,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {}
