// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;
import java.util.List;

/**
 * A type-checked {@code #>{db.t}#->delete(r | pred)} ({@code meta::legend::lite::delete}, 2026-10-18, the writes
 * design's L1): the table's rows the predicate holds for, deleted by the database ({@code DELETE FROM t WHERE pred}).
 *
 * @param table the store table
 * @param where the predicate, over the table's row
 * @param info  {@code Integer[1]}: the rows the database deleted
 */
public record TypedDelete(TypedTableReference table, TypedLambda where, ExprType info) implements TypedStoreWrite {

    @Override
    public List<TypedSpec> children() {
        return List.of(table, where);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 2, "TypedDelete");
        return new TypedDelete((TypedTableReference) kids.get(0), (TypedLambda) kids.get(1), info);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedDelete(table, where, info);
    }
}
