// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * A VALUE'S TEXT IN TWO FORMS ({@link SqlFn#TEXT_FORMS}; phase D's D1.3, 2026-11-15): the base renderer keeps the
 * product's, the first operand -- a list's JSON array (the user's ruling of 2026-11-10, start-here §5.4). The
 * compatibility edge's spelling passes, run first, keep legend-pure's.
 */
final class TextForms {

    private TextForms() {
    }

    /** The base renderer's pass: each {@link SqlFn#TEXT_FORMS} call its product form. */
    static final class Product extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlExpr expr(SqlExpr e) {
            // a value's two forms: the product's, the first (an execution's trace text left for the plan style,
            // B8 (ii), 2026-11-17)
            return e instanceof SqlExpr.Call c && c.fn() == SqlFn.TEXT_FORMS ? c.args().get(0) : e;
        }
    }
}
