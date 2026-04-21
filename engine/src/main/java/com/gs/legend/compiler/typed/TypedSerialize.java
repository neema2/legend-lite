package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Serialize a relational result to a scalar format (e.g., JSON string). */
public record TypedSerialize(
        TypedSpec source,
        String format,
        ExpressionType info
) implements TypedSpec {}
