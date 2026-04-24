package com.gs.legend.compiler.typed;

/**
 * A row/range offset as a window frame bound.
 *
 * <p>Value type is {@code double} to support fractional RANGE bounds
 * (e.g. {@code 0.5->_range(2.5)}). ROWS offsets are naturally integral
 * but share the representation; integral values round-trip exactly.
 * Sign convention: positive = FOLLOWING, negative = PRECEDING, zero =
 * CURRENT ROW (normalized by the checker to the {@link CurrentRow} bound).
 */
public record Offset(double value) implements TypedFrameBound {}
