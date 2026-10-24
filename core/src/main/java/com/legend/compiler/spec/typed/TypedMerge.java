// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;
import java.util.List;

/**
 * A type-checked {@code rel->merge(#>{db.t}#, ~[keys])} ({@code meta::legend::lite::merge}, 2026-10-18, the writes
 * design's L1): each of the relation's rows upserted into the table by its keys, the table's row of the same keys
 * updated to it, else the row inserted ({@code MERGE INTO t USING (rel) ON keys ...}).
 *
 * @param source the rows, their columns named as the table's
 * @param table  the store table
 * @param keys   the key columns, columns of both
 * @param info   {@code Integer[1]}: the rows the database merged
 */
public record TypedMerge(TypedSpec source, TypedTableReference table, TypedColSpecArray keys, ExprType info)
        implements TypedStoreWrite {

    @Override
    public List<TypedSpec> children() {
        return List.of(source, table, keys);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 3, "TypedMerge");
        return new TypedMerge(kids.get(0), (TypedTableReference) kids.get(1), (TypedColSpecArray) kids.get(2), info);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedMerge(source, table, keys, info);
    }
}
