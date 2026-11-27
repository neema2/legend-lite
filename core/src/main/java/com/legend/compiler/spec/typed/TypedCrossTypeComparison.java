// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;

import java.util.List;

/**
 * A COMPARISON ACROSS TYPES (B3 step 5, the one equality; the user's ruling of 2026-11-24, "Love it"): two values are
 * equal only when they have the same type and the same value, so a comparison whose two sides' types are known and
 * differ ({@code 1 == 1.0}, an enumeration against a String, a day against a date-time) is a mistake the compiler
 * catches. The typer hands such a call, as this record around the call it typed, to the door's comparison setting
 * ({@code compiler.spec.CrossTypeRule}; the edition program's step 2b, 2026-11-26) -- never a warning mode. Lite's
 * setting leaves the record in the query as a marker, which lite's planner refuses by name before anything is planned
 * ({@code compiler.spec.CrossTypeComparisons.refuse}); a compatibility door's answers it there -- legend-engine's the
 * database's comparison (the call itself), legend-pure's Pure's answer.
 *
 * @param comparison the call as the typer typed it ({@code equal}, {@code in}, {@code <}, ...)
 * @param kind       which comparison it is
 * @param operator   the comparison as the refusal writes it ({@code ==}, {@code in}, {@code <}, ...)
 * @param left       the first side's type
 * @param right      the second side's (an element's, for a collection side)
 * @param pos        where it was written; null for a call the compiler built
 */
public record TypedCrossTypeComparison(TypedSpec comparison, Kind kind, String operator, Type left, Type right,
        com.legend.protocol.@com.legend.base.Nullable SourceInfo pos) implements TypedSpec {

    /** The comparisons the rule reads. */
    public enum Kind {
        /** {@code ==}, {@code !=} (its {@code equal}), {@code eq}. */
        EQUALITY,
        /** {@code in}, {@code contains}: a value against a collection's elements. */
        MEMBERSHIP,
        /** {@code indexOf}: a value's position among a collection's elements. */
        INDEX,
        /** {@code <}, {@code <=}, {@code >}, {@code >=}, {@code compare}, two-argument {@code max} and {@code min}. */
        ORDERING,
        /** A test's assert of two sides: {@code assertEquals}, {@code assertNotEquals}, {@code assertEq},
         *  {@code assertNotEq}, {@code assertIs}, {@code assertIsNot}, {@code assertSameElements},
         *  {@code assertContains}, {@code assertNotContains}. Two sides of two known types are the mistake a
         *  comparison of them is (the user's ruling of 2026-11-26, "yes compile error": on lite's own door an assert
         *  across types is refused when the body is compiled; a compatibility door's assert is judged as it was). */
        ASSERT
    }

    /** Lite's refusal of this comparison: a compile error naming the two types and the fix (the planner's before it
     *  plans; the lowering's for one that reached it another way). */
    public com.legend.error.ModelException refusal() {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE, operator
                + " compares " + left.typeName() + " with " + right.typeName() + ": two values are equal only when"
                + " they have the same type and the same value, so this comparison is a mistake -- write both sides in"
                + " one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)");
    }

    /** Lite's refusal of this comparison where it is written in {@code function} between values whose types the
     *  CALL gives (the caller's own columns, a type variable's values): the same mistake as one written in the query,
     *  found where the function is used, so the message names the function and says the fix is the caller's (the
     *  user's ruling of 2026-11-27, "the split with compile error when we know"). */
    public com.legend.error.ModelException refusalAtTheCall(@com.legend.base.Nullable String function) {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE, operator
                + " compares " + left.typeName() + " with " + right.typeName() + " in "
                + (function == null ? "a function this query calls" : function) + ", between values whose types this"
                + " call gives: two values are equal only when they have the same type and the same value, so this call"
                + " is a mistake -- pass values of one type (toFloat(), toString(), an enumeration's own value)");
    }

    /** Lite's refusal of this comparison where a side is declared {@code Number}, {@code Any} or the abstract
     *  {@code Date} and the COLUMNS the two sides read hold two types that are never equal: known only once the
     *  query is planned over its store, so refused there, never answered false for every row (the user's ruling of
     *  2026-11-27, "great do it": the same reasoning as a comparison a call fixes). */
    public com.legend.error.ModelException refusalOverColumns() {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.LOWER, operator
                + " compares " + left.typeName() + " with " + right.typeName() + ", the types of the columns its two"
                + " sides read: two values are equal only when they have the same type and the same value, so this"
                + " comparison is a mistake -- convert one side (toFloat(), toDecimal(), toString())");
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
        TypedSpec.expectChildren(kids, 1, "TypedCrossTypeComparison");
        return new TypedCrossTypeComparison(kids.get(0), kind, operator, left, right, pos);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedCrossTypeComparison(comparison.withInfo(info), kind, operator, left, right, pos);
    }
}
