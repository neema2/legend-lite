package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.math.BigDecimal;

/** Decimal literal: {@code 3.14D}. */
public record TypedCDecimal(BigDecimal value, ExpressionType info) implements TypedSpec {}
