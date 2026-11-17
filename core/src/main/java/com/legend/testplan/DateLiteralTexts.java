// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * A DATE LITERAL'S TEXT IN THE DATABASE JUDGE'S FAILURE MESSAGE (census C1, the orchestrator's ruling of 2026-11-17:
 * the style decides): a two-form kind ({@code PlanStyle.twoForm}, {@link PlanStyleFacets#TWO_FORMS}), spelled once at
 * planning. The product's form, {@link LiteDateLiteralTexts#LITE}: lite's text (the host's {@code LiteWords.dateText});
 * the edge's: legend-pure's {@code toRepresentation}. Signatures only; the product's form is {@code LiteDateralTexts}.
 */
public interface DateLiteralTexts {

    /** The text of the date literal {@code d}. */
    String text(com.legend.values.PureDateLiteral d);
}
