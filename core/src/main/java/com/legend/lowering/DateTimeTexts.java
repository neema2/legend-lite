// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

/**
 * A DATETIME'S TEXT IN A MESSAGE, A TWO-FORM HOOK ({@code PlanStyle.twoForm}, census B9, the orchestrator's ruling of
 * 2026-11-17): the product's form is lite's canonical DateTime text, {@code DATETIME_TO_TEXT} (ISO 8601 in UTC, six
 * fraction digits and {@code Z}: measured equal to lite's writers' text at microsecond precision,
 * {@code result.CellText.productTime}; {@link Styled#LITE_DATE_TIMES}); the compatibility edge's is legend-pure's
 * print form, its override alone. A signature only.
 */
public interface DateTimeTexts {

    /** The text of the DateTime {@code x}; {@code written}, a literal's written text (the parser already shifted a
     *  zone-carrying literal to GMT), or null for a value read from the store. */
    SqlExpr dateTimeText(SqlExpr x, @com.legend.base.Nullable String written);
}
