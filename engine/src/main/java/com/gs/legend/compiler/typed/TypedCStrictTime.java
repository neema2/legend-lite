package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.time.LocalTime;

/** StrictTime literal: {@code %10:30:00}. */
public record TypedCStrictTime(LocalTime value, ExpressionType info) implements TypedSpec {}
