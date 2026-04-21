package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Boolean literal: {@code true} / {@code false}. */
public record TypedCBoolean(boolean value, ExpressionType info) implements TypedSpec {}
