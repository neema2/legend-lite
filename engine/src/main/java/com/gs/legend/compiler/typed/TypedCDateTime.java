package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.time.Instant;

/** DateTime literal: {@code %2024-01-15T10:30:00}. */
public record TypedCDateTime(Instant value, ExpressionType info) implements TypedSpec {}
