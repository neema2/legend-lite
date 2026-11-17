// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * THE PRODUCT'S TEXT OF A LIST (the user's ruling of 2026-11-10, start-here §5.4: a list's text is its JSON array; D1.3,
 * 2026-11-15): the product's form of {@link ListTexts}, the plan style's two-form kind (B8, 2026-11-17: until then the
 * first operand of TEXT_FORMS, which the product's renderer kept).
 */
public final class ProductListTexts implements ListTexts {

    public static final ProductListTexts INSTANCE = new ProductListTexts();

    private ProductListTexts() {
    }

    /** The variant's canonical JSON. */
    @Override
    public SqlExpr anyList(SqlExpr x) {
        return new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.TO_VARIANT,
                new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.JSON, SqlExpr.Reason.PRINT)),
                com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT);
    }

    /** Each element as its kind writes a JSON value, in a JSON array. */
    @Override
    public SqlExpr mixedList(Styled styled, SqlExpr.TaggedList tl) {
        return TaggedLists.json(styled, tl);
    }
}
