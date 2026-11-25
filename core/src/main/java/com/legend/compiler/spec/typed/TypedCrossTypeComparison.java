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
 * catches. The typer types such a call, written in the query or test body a door receives, as this marker around the
 * call it typed -- never a warning mode. Lite's planner refuses the marker by name before anything is planned
 * ({@code compiler.spec.CrossTypeComparisons.refuse}); a compatibility door rewrites it first -- legend-engine's to the
 * database's comparison (the call itself), legend-pure's to Pure's answer -- and lists what it loosened.
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
        ORDERING
    }

    /** Lite's refusal of this comparison: a compile error naming the two types and the fix (the planner's before it
     *  plans; the lowering's for one that reached it another way). */
    public com.legend.error.ModelException refusal() {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE, operator
                + " compares " + left.typeName() + " with " + right.typeName() + ": two values are equal only when"
                + " they have the same type and the same value, so this comparison is a mistake -- write both sides in"
                + " one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)");
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
