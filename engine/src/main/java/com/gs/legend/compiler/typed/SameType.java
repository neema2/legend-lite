package com.gs.legend.compiler.typed;

/** Fold strategy: reduce into the same element type (e.g., sum of integers). */
public record SameType() implements FoldStrategy {}
