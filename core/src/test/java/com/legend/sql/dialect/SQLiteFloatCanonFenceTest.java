// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The staged Float canon's text layer is fenced on SQLite (2026-11-16, PR1's finding B): without the fence, SQLite's
 * flattener copies the mantissa's matcher, a recursive common table expression, into every read of the digits and of
 * the point's position. An assertEquals over three doubles then took 0.6 s to prepare, and channel B's SQLite lane went
 * from 25 s to 103 s. Fenced, it prepares in 0.03 s. DuckDB plans the staged layers once and is not fenced.
 */
class SQLiteFloatCanonFenceTest {

    private static String canonOver(AnsiSqlRenderer dialect) {
        SqlExpr x = SqlExpr.Column.of("t", "x", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.PHYSICAL);
        return dialect.render(new SqlSelect(List.of(
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.FLOAT_CANON, x), "c", null)),
                false, new SqlSource.Table("t", "t", List.of(new OutputCol("x", SqlType.Scalar.DOUBLE, true)), false,
                        Map.of(), com.legend.sql.NameQuoting.BARE),
                null, List.of(), null, null, List.of(), null, null, List.of()));
    }

    @Test
    void theTextLayerIsFencedOnSQLite_once_perCanon() {
        String sql = canonOver(new SQLite());
        assertEquals(1, sql.split("OFFSET 0", -1).length - 1, sql);
        // the fence closes the layer that reads the text (the mantissa and the exponent), over the text's own layer
        assertTrue(sql.matches("(?s).*AS __fc0_t\\s+LIMIT -1\\s+OFFSET 0.*"), sql);
    }

    @Test
    void duckDbIsNotFenced() {
        assertFalse(canonOver(new DuckDb()).contains("OFFSET 0"));
    }
}
