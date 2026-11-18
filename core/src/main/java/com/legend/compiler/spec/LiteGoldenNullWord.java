// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

/**
 * THE PRODUCT'S FORM OF {@link GoldenNullWord} (GUARD's C.9, the orchestrator's ruling of 2026-11-17: a two-form kind
 * is types only, its product form a class of its own beside it). Lite's form: no word is a null cell.
 */
public enum LiteGoldenNullWord implements GoldenNullWord {
    NONE;

    @Override
    public boolean isNullWord(String cell) {
        return false;
    }
}
