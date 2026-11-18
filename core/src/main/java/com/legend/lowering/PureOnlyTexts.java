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
@com.legend.base.OpenLine("B8") public interface PureOnlyTexts {

    /** {@code format}'s {@code %0<width>d} of {@code value}: legend-pure pads the digits, then signs. */
    SqlExpr formatPadded(SqlExpr value, SqlExpr.IntLit width);

    /** An instance's {@code toString}, {@code id} its identity: legend-pure prints its repository id. */
    SqlExpr instanceText(SqlExpr id);

    /** legend-pure's {@code toRepresentation} of {@code x}, a value of type {@code t} (the TYPE-directed spelling: a
     *  string quoted with backslash escapes, a date its {@code %} form, a Decimal its {@code D}; C5's step A,
     *  2026-11-17: moved from the product's {@code lowering.Repr} to the edge). */
    SqlExpr representationOf(Styled styled, com.legend.compiler.element.type.Type t, SqlExpr x);

    /** {@code format}'s {@code %r} of {@code e}, the lowered {@code typed} (the NODE-directed spelling: a date literal
     *  its {@code %} form as written; census B12's 2b asks it). */
    SqlExpr formatR(Styled styled, com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSpec typed, SqlExpr e);

    /** A user's {@code toRepresentation}, {@code text} legend-pure's representation as the lowering writes it. */
    SqlExpr representation(SqlExpr text);
}
