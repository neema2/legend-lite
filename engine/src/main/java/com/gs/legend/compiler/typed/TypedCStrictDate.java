package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** StrictDate literal: {@code %2024-01-15}. Value is the raw textual form from the AST. */
public record TypedCStrictDate(String value, ExpressionType info) implements TypedSpec {}
