package com.gs.legend.compiler.typed;

/** A row/range offset as a window frame bound. */
public record Offset(long value) implements TypedFrameBound {}
