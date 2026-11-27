// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A COMPARISON WRITTEN IN A FUNCTION WHOSE SIDES' TYPES ARE KNOWN ONLY WHERE THE FUNCTION IS USED (the edition
 * program's step 6, 2026-11-27; the equality rule's run-time item, and the user's rulings of 2026-11-26 on its
 * decisions D3 and D5): {@code $x == $y}, {@code $x->in([0, 1])} or {@code $x < $y} in a body whose parameters are
 * declared {@code Number}, the abstract {@code Date}, {@code Any} or a type variable. Where the function is typed the
 * two types cannot be compared, so the typer hands the call, as this record around it, to the door's comparison
 * setting ({@code compiler.spec.CrossTypeRule.atRunTime}); a setting that answers such a comparison by its types keeps
 * the record until the functions a query reaches stand in the query, each argument in its parameter's place, and then
 * answers it from the types the sides have there ({@code CrossTypeRule.atTheCall}). The record never reaches the
 * lowering as anything but its call.
 *
 * @param comparison   the call as the typer typed it
 * @param kind         which comparison it is
 * @param operator     the comparison as a message writes it
 * @param leftWritten  whether the first side is number literals written in the function itself (a literal has no
 *                     type of its own: it reads as the type of the value it is written against, the literal rule)
 * @param rightWritten the same of the second side
 */
public record TypedRunTimeComparison(TypedSpec comparison, TypedCrossTypeComparison.Kind kind, String operator,
        boolean leftWritten, boolean rightWritten) implements TypedSpec {

    @Override
    public ExprType info() {
        return comparison.info();
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(comparison);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1, "TypedRunTimeComparison");
        return new TypedRunTimeComparison(kids.get(0), kind, operator, leftWritten, rightWritten);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedRunTimeComparison(comparison.withInfo(info), kind, operator, leftWritten, rightWritten);
    }
}
