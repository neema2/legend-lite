package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Enum value reference: {@code JoinKind.INNER}, {@code DurationUnit.DAYS}. */
public record TypedEnumValue(String enumType, String member, ExpressionType info) implements TypedSpec {}
