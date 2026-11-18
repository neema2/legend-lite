// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * THE PRODUCT'S FORM OF {@link RenderedTextRule} (GUARD's C.9, the orchestrator's ruling of 2026-11-17: a two-form kind
 * is types only, its product form a class of its own beside it). Lite's form: the two strings are the verdict.
 */
public enum LiteRenderedTextRule implements RenderedTextRule {
    STRINGS;

    @Override
    public boolean ruledWhereTheyDiffer() {
        return false;
    }
}
