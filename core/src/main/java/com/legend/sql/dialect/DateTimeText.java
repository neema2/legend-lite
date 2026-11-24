// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * THE PRODUCT'S TEXT OF A DATETIME ({@link SqlFn#DATETIME_TO_TEXT}; the user's ruling of 2026-11-10, D_BRIEF §1; phase
 * D's D1.2, 2026-11-15; amended 2026-11-22, the user: "Yes always pretty print to iso nanos"): ISO 8601 in UTC, nine
 * fraction digits and {@code Z}, the database's own STRFTIME of the value -- each dialect spells the format's parts
 * ({@code SUBSEC_NANO} on every one, a database holding microseconds padding three zeros). A pass of the base renderer, run first,
 * so every later pass sees the STRFTIME; a literal's written text (the call's second operand) is the compatibility
 * edge's, never read here.
 */
final class DateTimeText {

    private DateTimeText() {
    }

    /** The base renderer's pass: each {@link SqlFn#DATETIME_TO_TEXT} call its STRFTIME. */
    static final class Expansion extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.DATETIME_TO_TEXT) {
                return SqlExpr.Call.of(SqlFn.STRFTIME, expr(c.args().get(0)),
                        new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_NANO_Z));
            }
            return super.expr(e);
        }
    }
}
