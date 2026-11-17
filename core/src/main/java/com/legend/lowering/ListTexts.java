// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

/**
 * A LIST'S TEXT, A TWO-FORM KIND OF THE PLAN STYLE (the separation census's B8, 2026-11-17: TEXT_FORMS left the MIR;
 * {@code PlanStyle.twoForm}, E-SQL's, the orchestrator's ruling): the product's style answers its own canonical form
 * ({@link ProductListTexts}: the list's JSON array, the user's ruling of 2026-11-10), the compatibility edge's
 * legend-pure's {@code [a, b]} ({@code edge.PureListTexts}). Signatures only.
 */
public interface ListTexts {

    /** The text of a list carried as a variant under {@code Any} ({@code x}, known to be a JSON array). */
    SqlExpr anyList(SqlExpr x);

    /** The text of a mixed list {@code tl}, each element printed under {@code styled}. */
    SqlExpr mixedList(Styled styled, SqlExpr.TaggedList tl);
}
