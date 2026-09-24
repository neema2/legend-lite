// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * A body refused BY DECISION (the compiler's WalledBodies list): the
 * platform implements that concern itself, so the upstream body is never
 * typed or inlined. A {@link NotImplementedException} to every caller that
 * treats it as one; its own type to a caller that must tell a decision from a
 * gap — never the message text.
 */
public final class WalledBodyException extends NotImplementedException {

    public WalledBodyException(String message) {
        super(message);
    }
}
