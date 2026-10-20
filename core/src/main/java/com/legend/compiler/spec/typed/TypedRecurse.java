// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;
import java.util.List;

/**
 * A type-checked {@code meta::legend::lite::recurse(base, r | step)} (2026-10-17, the Cypher island's unbounded
 * variable-length patterns): the base relation and the step, a lambda over a relation of the base's type yielding
 * one of the same type. {@link #info()} is the base's relation type (the signature's {@code Relation<T>}). The
 * lowerer spells a recursive common table expression: the base UNION ALL the step over the expression's own name.
 *
 * @param source the base relation
 * @param step   the step: its one parameter the previous round's rows, its body their successors
 * @param info   the result type, the base's
 */
public record TypedRecurse(TypedSpec source, TypedLambda step, ExprType info) implements TypedRelationOp {

    @Override
    public List<TypedSpec> children() {
        return List.of(source, step);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 2, "TypedRecurse");
        return new TypedRecurse(kids.get(0), (TypedLambda) kids.get(1), info);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedRecurse(source, step, info);
    }
}
