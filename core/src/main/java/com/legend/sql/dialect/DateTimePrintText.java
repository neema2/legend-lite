// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;

/**
 * {@link SqlFn#DATETIME_PRINT}, legend-pure's print form of a DateTime, spelled over the database's own STRFTIME. Since
 * census B9 (2026-11-17, the orchestrator's ruling (a)) the JUDGES' CANON's alone: a message's DateTime text is the
 * two-form hook {@code lowering.DateTimeTexts} (lite's DATETIME_TO_TEXT in the product, this node only at the edge),
 * and the one product emitter left is {@code LiteralSpelling.temporalCanon}, the database judge's canonical text of a
 * DateTime, which must equal the host judge's (judge parity). It goes with census C5's step C (E-P5's: canons as typed
 * comparisons), when this composition moves to the edge or is deleted. Moved unchanged from the lowering's
 * {@code LiteralSpelling.dateTimeText} (phase D's D1.2, 2026-11-15). A pass of the base renderer, run first.
 */
final class DateTimePrintText {

    private DateTimePrintText() {
    }

    /** The base renderer's pass: each {@link SqlFn#DATETIME_PRINT} call its composition. */
    static final class Expansion extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.DATETIME_PRINT) {
                return c.args().size() > 1 && c.args().get(1) instanceof SqlExpr.StringLit written
                        ? new SqlExpr.StringLit(written.value() + "+0000")
                        : of(expr(c.args().get(0)));
            }
            return super.expr(e);
        }
    }

    /**
     * Pure's print form of a DateTime read from a database (docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §4, the one owner,
     * 2026-10-18) -- {@code T}, the fraction's digits with their trailing zeros trimmed (a timestamp carries no
     * written precision: {@code .123} prints {@code .123}, {@code .123456789} all nine, a whole second none),
     * {@code +0000}. The fraction is read at nine digits, which every SQL dialect spells.
     */
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
                        SqlExpr.Call.of(SqlFn.STRFTIME, x,
                                new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_LOCAL)),
                        fraction),
                new SqlExpr.StringLit("+0000"));
    }
}
