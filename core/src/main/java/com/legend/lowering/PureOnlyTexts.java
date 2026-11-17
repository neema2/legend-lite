// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

/**
 * TEXTS ONLY LEGEND-PURE WRITES, A LOWERING FACET OF THE PLAN STYLE (the separation census's B8 (i), 2026-11-17: the
 * orchestrator's go on F's design note; the user's rulings of 2026-11-10, "format: %s %d %f %t{} in the product; %r and
 * padding at the edge"): the compatibility edge composes each from the product's own nodes; the product's style
 * refuses each by name at lowering, with the call's own refusal ({@link TextRefusals}). A hook (HookRulingTest):
 * signatures only.
 */
public interface PureOnlyTexts {

    /** {@code format}'s {@code %0<width>d} of {@code value}: legend-pure pads the digits, then signs. */
    SqlExpr formatPadded(SqlExpr value, SqlExpr.IntLit width);

    /** A user's {@code toRepresentation}, {@code text} legend-pure's representation as the lowering writes it. */
    SqlExpr representation(SqlExpr text);
}
