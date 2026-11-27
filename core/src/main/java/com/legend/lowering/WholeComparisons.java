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
}
