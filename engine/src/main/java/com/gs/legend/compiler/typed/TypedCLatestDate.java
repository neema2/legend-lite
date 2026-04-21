package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Latest-date sentinel: {@code %latest} (unbounded upper temporal bound). */
public record TypedCLatestDate(ExpressionType info) implements TypedSpec {}
