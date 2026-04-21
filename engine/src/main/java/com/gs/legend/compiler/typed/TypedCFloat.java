package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Float literal: {@code 3.14}. */
public record TypedCFloat(double value, ExpressionType info) implements TypedSpec {}
