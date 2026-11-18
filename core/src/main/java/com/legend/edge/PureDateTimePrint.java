// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;

/**
 * legend-pure's print form of a DateTime, composed by the edge from the product's own nodes (census B12, 2026-11-17:
 * moved from the product's {@code sql.dialect.DateTimePrintText}, unchanged; the {@code DATETIME_PRINT} node it expanded
 * left the MIR). Written by the edge's print pass for the product's {@code DATETIME_TO_TEXT} ({@link PurePrintForms}); a
 * literal's text is its written text ({@link EdgeDateTimeTexts}).
 */
final class PureDateTimePrint {

    private PureDateTimePrint() {
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
