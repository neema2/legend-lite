package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Let binding: {@code let name = value;} inside a lambda body. */
public record TypedLet(
        String name,
        TypedSpec value,
        ExpressionType info
) implements TypedSpec {}
