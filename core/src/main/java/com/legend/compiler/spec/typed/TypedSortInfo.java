// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A checked sort key: {@code asc(~col)} / {@code desc(~col)}, optionally with an
 * explicit null placement ({@code ascending(~col, NullOrder.FIRST)},
 * {@code ~col->ascending()->emptyLast()} — upstream's {@code SortInfo.nullOrder},
 * 4.145.0). A key without one rides the engine's canonical placement.
 *
 * @param column    the sort column's name
 * @param ascending {@code true} for {@code asc}/{@code ascending}
 * @param nullOrder the explicit null placement, or null for the default
 * @param info      {@code SortInfo<(column:…)>[1]}, from the signature
 */
public record TypedSortInfo(String column, boolean ascending,
        @com.legend.Nullable NullOrder nullOrder, ExprType info) implements TypedSpec {

    /** Upstream's {@code meta::pure::functions::relation::NullOrder}. */
    public enum NullOrder { FIRST, LAST }

    @Override
    public List<TypedSpec> children() {
        return List.of();
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 0, "TypedSortInfo");
        return this;
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedSortInfo(column, ascending, nullOrder, info);
    }
}
