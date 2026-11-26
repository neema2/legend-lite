// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;

/**
 * A DateTime's text at the edge, the edge's form of the two-form hook {@link com.legend.lowering.DateTimeTexts}
 * (census B9, 2026-11-17). A LITERAL's text is its written text and {@code +0000}, as legend-pure prints it (the
 * parser already shifted it to GMT; census B12, 2026-11-17, the orchestrator's ruling on K15's shape): a constant, no
 * SQL computes it. Any other value is the product's own node, {@code DATETIME_TO_TEXT}, the value alone, which the
 * edge's dialects write as the product's do: lite's own text (the edition program's step 8, 2026-11-26; the user's
 * ruling of that day, "go with lite in the middle" -- until then a pass of the edge's dialects composed legend-pure's
 * print for it in SQL, {@code PureDateTimePrint}, deleted). legend-pure's print of a computed DateTime is Java's at a
 * last step ({@link PrintedRelations}, {@link PureValueText#dateTime}).
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
