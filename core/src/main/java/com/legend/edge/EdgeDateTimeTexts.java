// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;

/**
 * legend-pure's print form of a DateTime in a message, the edge's form of the two-form hook
 * {@link com.legend.lowering.DateTimeTexts} (census B9, 2026-11-17), composed here at lowering ({@link PureDateTimePrint};
 * census B12, 2026-11-17: the DATETIME_PRINT node and the base renderer's pass that expanded it are gone), a literal's
 * written text printed as written.
 */
final class EdgeDateTimeTexts implements com.legend.lowering.DateTimeTexts {

    static final EdgeDateTimeTexts INSTANCE = new EdgeDateTimeTexts();

    private EdgeDateTimeTexts() {
    }

    @Override
    public SqlExpr dateTimeText(SqlExpr x, @com.legend.base.Nullable String written) {
        return PureDateTimePrint.of(x, written);
    }
}
