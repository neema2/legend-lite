// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.base.Nullable;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.sql.SqlQuery;

/**
 * HOW A COMPARISON OF TWO WHOLE SIDES IS ANSWERED (the edition program's step 6, its closing batch, 2026-11-27): the
 * product's rule for {@code ==} and {@code hasSameElements} over two computed lists, two maps or two tables is a
 * STATEMENT -- each side planned once, as the statement it is, and the two compared by the code that judges an
 * assert of them -- and a statement is the planner's to make ({@code planner.RootComparison}). The planner hands the
 * lowering this, with no condition on who is asking.
 *
 * <p>The LOWERING decides, at the call, by the one lookup it has ({@link Scalars#doorsRule}, the door's table): a
 * rule the door named for the call replaces the call's own rule, this statement with it; where the door named none,
 * the comparison is the product's own and the lowering asks here. The planner knows nothing of doors (the user,
 * 2026-11-27: "We shouldn't land with the planner knowing editions"; until then the planner read which number rule
 * the door's table had named and inferred the door from it).
 *
 * <p>One answer wherever the comparison stands: as the query's last statement the planner's statement IS the
 * statement; inside an expression the lowering reads the same statement as one value.
 */
public interface WholeComparisons {

    /** The statement whose one row answers {@code comparison} (under {@code negated}, its {@code !}), the body's
     *  LAST statement, which nothing encloses; null where the planner plans no whole sides for it (two single values,
     *  two known types): the comparison is then lowered as any expression is. */
    @Nullable SqlQuery asTheStatement(TypedNativeCall comparison, boolean negated);

    /** The statement whose one row answers {@code comparison} met INSIDE an expression (an {@code if}'s condition, an
     *  operand of {@code &&}, a column's value); null as {@link #asTheStatement}, and where a side reads the row or
     *  the element the comparison stands in (no statement of its own answers that). */
    @Nullable SqlQuery insideAnExpression(TypedNativeCall comparison);

    /** A statement whose one value is read from EVERY value of {@code side}, a computed list (NULL where it holds
     *  none): what a comparison of two types, answered "never equal" by the types alone, still evaluates of that
     *  side -- Pure evaluates both sides before it compares them, so a side that fails raises
     *  ({@code SideEqualities.everyValueRead}). Null where {@code side} reads the row or the element the comparison
     *  stands in, or its values have no typed column: no statement of its own reads it. */
    @Nullable SqlQuery everyValueRead(com.legend.compiler.spec.typed.TypedSpec side);

    /** {@code side}'s values as typed rows, a statement of its own ({@code SideEqualities.typedRows}: each value and
     *  its place in the side's order): what a dialect that compares two sides as rows reads for the one "same
     *  values" instruction (step 6B, slice 1; {@code Equalities.asSameValues}). Null where {@code side} reads the row
     *  or the element the comparison stands in, draws a value afresh where it is written, or its values have no
     *  typed column: no statement of its own gives its rows. */
    @Nullable SqlQuery sideRows(com.legend.compiler.spec.typed.TypedSpec side);

    /** Whether every instance {@code side} holds has an identity the product tracks, proven from the side's own
     *  rows ({@code SideEqualities.tracked}): its identity is then read plain, never behind the guard that raises
     *  where the product does not track it. False where the side has no rows of its own. */
    default boolean tracksIdentity(com.legend.compiler.spec.typed.TypedSpec side) {
        SqlQuery rows = sideRows(side);
        return rows != null && SideEqualities.tracked(rows);
    }
}
