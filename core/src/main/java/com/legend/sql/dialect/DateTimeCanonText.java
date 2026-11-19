// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;

/**
 * {@link SqlFn#DATETIME_CANON_TEXT}, the judges' canon of a timestamp in LITE'S spelling (census C5's step C,
 * 2026-11-17): {@code T}-separated, the fraction's digits with their trailing zeros trimmed, {@code Z}. A pass of the
 * base renderer, run first, so a plan's tree carries one node and every later pass sees its composition, spelled from
 * STRFTIME at nine digits, which every SQL dialect spells.
 */
final class DateTimeCanonText {

    private DateTimeCanonText() {
    }

    /** The base renderer's pass: each {@link SqlFn#DATETIME_CANON_TEXT} call its composition. */
    static final class Expansion extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.DATETIME_CANON_TEXT) {
                return of(expr(c.args().get(0)));
            }
            return super.expr(e);
        }
    }

    /** The canon of the timestamp {@code x}. */
    static SqlExpr of(SqlExpr x) {
        SqlExpr digits = SqlExpr.Call.of(SqlFn.RTRIM,
                SqlExpr.Call.of(SqlFn.STRFTIME, x,
                        new SqlExpr.FormatLit(List.of(com.legend.sql.DateFmt.Part.SUBSEC_NANO))),
                new SqlExpr.StringLit("0"));
        SqlExpr fraction = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, digits, new SqlExpr.StringLit("")), new SqlExpr.StringLit(""))),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("."), digits), SqlExpr.Reason.PRINT);
        return SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT,
                        SqlExpr.Call.of(SqlFn.STRFTIME, x, new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_LOCAL)),
                        fraction),
                new SqlExpr.StringLit("Z"));
    }
}
