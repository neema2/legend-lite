package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.time.LocalDate;

/** StrictDate literal: {@code %2024-01-15}. */
public record TypedCStrictDate(LocalDate value, ExpressionType info) implements TypedSpec {}
