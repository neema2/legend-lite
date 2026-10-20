// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import java.util.List;
import org.junit.jupiter.api.Test;

/** A NULL cast to the Variant renders TYPED on every dialect that types its JSON carrier: Postgres's
 *  {@code to_jsonb(NULL)} has no type to go on and its jsonb operators over an untyped NULL are ambiguous
 *  ({@code operator is not unique: unknown #>> unknown}), so the island's typed null Variant column (a class
 *  lacking a property typed two ways, docs/CYPHER_STUDY_2026_10_15.md §12.62) spells its type (2026-10-17). */
class NullVariantCastTest {

    private static SqlSelect select() {
        OutputCol v = new OutputCol("v", SqlType.Scalar.JSON, true, OutputCol.Origin.DERIVED);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.NullLit(),
                SqlType.Scalar.JSON), "v", v)), false, new SqlSource.Dual(), null, List.of(), null, null, List.of(),
                null, null, List.of(v));
    }

    @Test
    void aNullCastToAVariantIsTyped() {
        assertEquals("SELECT CAST(NULL AS JSONB) AS \"v\"", new Postgres().render(select()));
        assertEquals("SELECT CAST(NULL AS JSON) AS v", new DuckDb().render(select()));
    }
}
