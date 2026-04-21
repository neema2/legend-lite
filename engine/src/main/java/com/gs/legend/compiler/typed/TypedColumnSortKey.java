package com.gs.legend.compiler.typed;

/** Sort by column name. */
public record TypedColumnSortKey(String column, SortDirection direction) implements TypedSortKey {}
