package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** String literal: {@code 'hello'}. */
public record TypedCString(String value, ExpressionType info) implements TypedSpec {}
