// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;

/**
 * legend-pure's print form of a DateTime in a message, the edge's form of the two-form hook
 * {@link com.legend.lowering.DateTimeTexts} (census B9, 2026-11-17). Census B12 (2026-11-17, the orchestrator's ruling on
 * K15's shape): a literal's text is its written text, as legend-pure prints it (the parser already shifted it to GMT);
 * any other value is the product's own node, {@code DATETIME_TO_TEXT}, the value alone, which the edge's dialects print
 * legend-pure's way ({@link PurePrintForms}, {@link PureDateTimePrint}). No operand only the edge reads reaches the
 * product's node, and the DATETIME_PRINT node is gone.
 */
final class EdgeDateTimeTexts implements com.legend.lowering.DateTimeTexts {

    static final EdgeDateTimeTexts INSTANCE = new EdgeDateTimeTexts();

    private EdgeDateTimeTexts() {
    }

    @Override
    public SqlExpr dateTimeText(SqlExpr x, @com.legend.base.Nullable String written) {
        return written != null ? new SqlExpr.StringLit(written + "+0000")
                : SqlExpr.Call.of(com.legend.sql.SqlFn.DATETIME_TO_TEXT, x);
    }
}
