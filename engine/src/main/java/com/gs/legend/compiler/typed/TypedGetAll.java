package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** {@code ClassName.all()} — the root relation for a mapped class. */
public record TypedGetAll(String className, ExpressionType info) implements TypedSpec {}
