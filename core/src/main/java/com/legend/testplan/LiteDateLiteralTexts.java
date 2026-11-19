// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * THE PRODUCT'S FORM OF {@link DateLiteralTexts} (GUARD's C.9, the orchestrator's ruling of 2026-11-17: a two-form kind
 * is types only, its product form a class of its own beside it). Lite's form: its day where it has no time of day, else
 * ISO 8601 in UTC with six fraction digits and Z.
 */
public enum LiteDateLiteralTexts implements DateLiteralTexts {
    LITE;

    @Override
    public String text(com.legend.values.PureDateLiteral d) {
        return DatabaseJudge.liteDate(d);
    }
}
