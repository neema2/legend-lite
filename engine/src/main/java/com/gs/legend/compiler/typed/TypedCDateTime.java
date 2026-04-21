package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** DateTime literal: {@code %2024-01-15T10:30:00}. Value is the raw textual form from the AST. */
public record TypedCDateTime(String value, ExpressionType info) implements TypedSpec {}
