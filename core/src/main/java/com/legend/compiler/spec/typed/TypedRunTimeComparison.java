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
 * answers it from the types the sides have there ({@code CrossTypeRule.atTheCall}). The record reaches the
 * lowering as its call, or, answered "never equal" with a side that may raise ({@link #answeredNeverEqual}), as the
 * false that still evaluates its sides.
 *
 * @param comparison   the call as the typer typed it
 * @param kind         which comparison it is
 * @param operator     the comparison as a message writes it
 * @param leftWritten  whether the first side is number literals written in the function itself (a literal has no
 *                     type of its own: it reads as the type of the value it is written against, the literal rule)
 * @param rightWritten the same of the second side
 * @param fixedByTheCall whether each call FIXES the two sides' types: neither side is declared {@code Number}, the
 *                     abstract {@code Date} or {@code Any} -- the author's word that it is compared when the query
 *                     runs -- so the sides are the caller's own columns or a type variable's values, known where the
 *                     function is used. Two different types there are a mistake of that call, refused when the query
 *                     is compiled (the user's ruling of 2026-11-27: "the split with compile error when we know")
 * @param function     the function the comparison is written in, for a message; null where the typer names none
 * @param neverEqual   whether the comparison is answered: its sides hold two types, so it is false, its sides still
 *                     evaluated ({@link #answeredNeverEqual})
 */
public record TypedRunTimeComparison(TypedSpec comparison, TypedCrossTypeComparison.Kind kind, String operator,
        boolean leftWritten, boolean rightWritten, boolean fixedByTheCall,
        @com.legend.base.Nullable String function, boolean neverEqual) implements TypedSpec {

    /** A comparison kept for its call, not yet answered. */
    public TypedRunTimeComparison(TypedSpec comparison, TypedCrossTypeComparison.Kind kind, String operator,
            boolean leftWritten, boolean rightWritten, boolean fixedByTheCall,
            @com.legend.base.Nullable String function) {
        this(comparison, kind, operator, leftWritten, rightWritten, fixedByTheCall, function, false);
    }

    /** This record, written in {@code function}. */
    public TypedRunTimeComparison in(@com.legend.base.Nullable String function) {
        return new TypedRunTimeComparison(comparison, kind, operator, leftWritten, rightWritten, fixedByTheCall,
                function, neverEqual);
    }

    /** This comparison ANSWERED by its sides' types -- two types, never equal -- where a side is a computation that
     *  may raise ({@code [1, 2]->at(5)->cast(@Number) == 1.0}): the answer is false, and the database still evaluates
     *  both sides, so an error a side raises is raised as Pure raises it (the user, 2026-11-27: "agreed"; the outside
     *  audit's finding that the constant false dropped it). The one form of this record the lowering writes
     *  ({@code lowering.Equalities.neverEqualButEvaluated}). */
    public TypedRunTimeComparison answeredNeverEqual() {
        return new TypedRunTimeComparison(comparison, kind, operator, leftWritten, rightWritten, false, function,
                true);
    }

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
        return new TypedRunTimeComparison(kids.get(0), kind, operator, leftWritten, rightWritten, fixedByTheCall,
                function, neverEqual);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedRunTimeComparison(comparison.withInfo(info), kind, operator, leftWritten, rightWritten,
                fixedByTheCall, function, neverEqual);
    }
}
