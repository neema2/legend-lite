// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * A RENDERER CAPABILITY WALL (H2_BACKEND.md §9/§10): the SQL IR asked a
 * dialect for a construct it has no spelling for — an HONEST, EXPECTED,
 * registrable outcome, distinct from a wrong answer. The portability
 * sweep classifies these UNSUPPORTED (its capability budget); every
 * other failure stays FAIL/ERROR. Extends {@link IllegalStateException}
 * so pre-existing catch sites are unaffected.
 */
public final class DialectCapability extends IllegalStateException {

    public DialectCapability(String message) {
        super(message);
    }
}
