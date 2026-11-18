// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;

/**
 * THE PRODUCT'S FORM OF {@link VerdictNumbers} (train 17, 2026-11-18): lite's exact rule -- a Float compared exactly
 * (S15/S132, the user's ruling of 2026-11-05: "I like our clean float"), a Decimal by its value, as {@code ==} does in
 * the database (register S139, the user's ruling of 2026-11-05).
 */
public enum LiteVerdictNumbers implements VerdictNumbers {
    EXACT;

    @Override
    public boolean floatTolerance() {
        return false;
    }

    @Override
    public SqlExpr floatsWithin(SqlExpr x, SqlExpr y) {
        throw new IllegalStateException("lite compares a Float exactly: no tolerance is planned");
    }

    @Override
    public boolean decimalByValue(Type kind) {
        return kind == Type.Primitive.DECIMAL || kind instanceof Type.PrecisionDecimal;
    }
}
