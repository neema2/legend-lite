// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.compiler.element.type.Type;

/**
 * THE PRODUCT'S FORM OF {@link CrossKindNumbers} (GUARD's C.9, the orchestrator's ruling of 2026-11-17: a two-form kind
 * is types only, its product form a class of its own beside it). The product's form: two numbers of different fine
 * kinds are never equal.
 */
public enum LiteCrossKindNumbers implements CrossKindNumbers {
    NEVER;

    @Override
    public boolean byText(Type e, Type a) {
        return false;
    }
}
