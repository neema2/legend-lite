// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE FLOAT CARRIER'S HOOK (Phase 8, 2026-11-16): a Float carried as text (a carrier's cast) is written by the
 * dialect's exact form, a Float printed is not -- SQLite's seventeen digits, where its own cast writes fifteen; DuckDB's
 * own cast, which reads back (measured).
 */
class FloatCarrierHookTest {

    private static final OutputCol D = new OutputCol("D", SqlType.Scalar.DOUBLE, true);
    private static final OutputCol T = new OutputCol("t", SqlType.Scalar.VARCHAR, true);

    private static SqlQuery castOf(SqlExpr.Reason reason) {
        SqlSource.Table table = new SqlSource.Table("T", "t0", List.of(D), false, Map.of(), NameQuoting.BARE);
        SqlExpr cast = new SqlExpr.Cast(SqlExpr.Column.of("t0", D), SqlType.Scalar.VARCHAR, reason);
        return new SqlSelect(List.of(new SqlSelect.Projection(cast, "t", T)), false, table, null, List.of(), null, null,
                List.of(), null, null, List.of(T));
    }

    @Test
    void sqliteCarriesAFloatAsSeventeenDigits_andPrintsItAsBefore() {
        // the carrier's own form (a print reaches SQLite's shortest-digit emulation, which tries 17 digits too)
        String carrier = "IS NULL THEN NULL ELSE printf('%!.17g', t0.D) END";
        assertTrue(new SQLite().render(castOf(SqlExpr.Reason.CARRIER)).contains(carrier),
                new SQLite().render(castOf(SqlExpr.Reason.CARRIER)));
        assertFalse(new SQLite().render(castOf(SqlExpr.Reason.USER)).contains(carrier));
    }

    @Test
    void duckDbCarriesAFloatByItsOwnCast() {
        String sql = new DuckDb().render(castOf(SqlExpr.Reason.CARRIER));
        assertTrue(sql.contains("CAST("), sql);
        assertFalse(sql.contains("printf"), sql);
    }
}
