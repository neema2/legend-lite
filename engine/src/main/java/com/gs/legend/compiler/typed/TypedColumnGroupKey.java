package com.gs.legend.compiler.typed;

/** Group by a column name. */
public record TypedColumnGroupKey(String column, String alias) implements TypedGroupKey {}
