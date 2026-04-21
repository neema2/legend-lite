package com.gs.legend.compiler.typed;

/**
 * Window frame bound: unbounded, current row, or a row/range offset.
 */
public sealed interface TypedFrameBound permits Unbounded, CurrentRow, Offset {}
