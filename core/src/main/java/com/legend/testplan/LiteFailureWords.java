// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * THE PRODUCT'S FORM OF {@link FailureWords} (GUARD's C.9, the orchestrator's ruling of 2026-11-17: a two-form kind is
 * types only, its product form a class of its own beside it): lite's own sentences.
 */
public enum LiteFailureWords implements FailureWords {
    LITE;

    @Override
    public boolean upstreamWords() {
        return false;
    }
}
