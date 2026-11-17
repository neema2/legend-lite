// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * THE PRODUCT'S FORM OF {@link RenderedTexts} (GUARD's C.9, the orchestrator's ruling of 2026-11-17: a two-form kind is
 * types only, its product form a class of its own beside it). Lite's form: two strings that differ are never held.
 */
public enum LiteRenderedTexts implements RenderedTexts {
    STRINGS;

    @Override
    public boolean held(String expected, String actual, String form, boolean sorted) {
        return false;
    }
}
