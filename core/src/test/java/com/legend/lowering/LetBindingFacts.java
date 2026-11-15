// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

/** P1a's binding rules read by the lean census (test side): whether a repeat can raise, and whether P1a may bind it. */
public final class LetBindingFacts {

    private LetBindingFacts() {
    }

    public static boolean canRaise(SqlExpr e) {
        return LetBinding.canRaise(e);
    }

    public static boolean eligible(SqlExpr e) {
        return LetBinding.eligible(e);
    }
}
