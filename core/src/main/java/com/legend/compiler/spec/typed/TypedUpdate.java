// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;
import java.util.List;

/**
 * A type-checked {@code #>{db.t}#->update(r | pred, ~[c: r | expr])} ({@code meta::legend::lite::update}, 2026-10-18,
 * the writes design's L1): the columns of the table's rows the predicate holds for set, each to its expression over the
 * row as it was ({@code UPDATE t SET c = expr WHERE pred}).
 *
 * @param table the store table
 * @param where the predicate, over the table's row
 * @param set   the columns set, each named as the table's and its expression over the row
 * @param info  {@code Integer[1]}: the rows the database updated
 */
public record TypedUpdate(TypedTableReference table, TypedLambda where, TypedFuncColSpecArray set, ExprType info)
        implements TypedStoreWrite {

    @Override
    public List<TypedSpec> children() {
        return List.of(table, where, set);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 3, "TypedUpdate");
        return new TypedUpdate((TypedTableReference) kids.get(0), (TypedLambda) kids.get(1),
                (TypedFuncColSpecArray) kids.get(2), info);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedUpdate(table, where, set, info);
    }
}
