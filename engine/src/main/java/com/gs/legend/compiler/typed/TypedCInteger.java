package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Integer literal: {@code 42}. */
public record TypedCInteger(long value, ExpressionType info) implements TypedSpec {}
