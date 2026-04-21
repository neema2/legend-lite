package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** StrictTime literal: {@code %10:30:00}. Value is the raw textual form from the AST. */
public record TypedCStrictTime(String value, ExpressionType info) implements TypedSpec {}
