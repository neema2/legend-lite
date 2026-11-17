// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * A DATE LITERAL'S TEXT IN THE DATABASE JUDGE'S FAILURE MESSAGE (census C1, the orchestrator's ruling of 2026-11-17:
 * the style decides): a two-form kind ({@code PlanStyle.twoForm}, {@link PlanStyleFacets#TWO_FORMS}), spelled once at
 * planning. The product's form, {@link Product#LITE}: lite's text (the host's {@code LiteWords.dateText}); the edge's:
 * legend-pure's {@code toRepresentation}. Signatures only, and the product's form.
 */
public interface DateLiteralTexts {

    /** Lite's form: its day where it has no time of day, else ISO 8601 in UTC with six fraction digits and Z. */
    enum Product implements DateLiteralTexts {
        LITE;

        @Override
        public String text(com.legend.values.PureDateLiteral d) {
            return DatabaseJudge.liteDate(d);
        }
    }

    /** The text of the date literal {@code d}. */
    String text(com.legend.values.PureDateLiteral d);
}
