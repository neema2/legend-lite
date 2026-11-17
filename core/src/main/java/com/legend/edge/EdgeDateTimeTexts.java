// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * legend-pure's print form of a DateTime in a message, the edge's form of the two-form hook
 * {@link com.legend.lowering.DateTimeTexts} (census B9, 2026-11-17): the {@link SqlFn#DATETIME_PRINT} node, a literal's
 * written text its second operand, composed by the edge's spelling pass ({@link DateTimePrintText}).
 */
final class EdgeDateTimeTexts implements com.legend.lowering.DateTimeTexts {

    static final EdgeDateTimeTexts INSTANCE = new EdgeDateTimeTexts();

    private EdgeDateTimeTexts() {
    }

    @Override
    public SqlExpr dateTimeText(SqlExpr x, @com.legend.base.Nullable String written) {
        return written == null ? SqlExpr.Call.of(SqlFn.DATETIME_PRINT, x)
                : SqlExpr.Call.of(SqlFn.DATETIME_PRINT, x, new SqlExpr.StringLit(written));
    }
}
