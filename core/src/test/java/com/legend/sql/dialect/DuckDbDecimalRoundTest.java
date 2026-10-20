// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure's half-even round of a decimal to a constant scale on DuckDB, exactly (IQ island, 2026-10-17, the corpus lane's
 * wide CASTs): DuckDB's ROUND_EVEN of a DECIMAL is a DOUBLE, so 1524157887653619.89 came back .84; its ROUND of a
 * DECIMAL is exact, half away from zero, and an exact half whose rounding is odd steps back to its even neighbour.
 */
class DuckDbDecimalRoundTest {

    private static BigDecimal rounded(String value, long places) throws Exception {
        SqlExpr round = SqlExpr.Call.of(SqlFn.ROUND, new SqlExpr.DecimalLit(new BigDecimal(value)),
                new SqlExpr.IntLit(places));
        SqlSelect q = SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(new SqlSelect.Projection(round,
                "a", null)));
        String sql = new DuckDb().render(q);
        assertTrue(!sql.contains("ROUND_EVEN"), sql);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBigDecimal(1);
        }
    }

    @Test
    void anExactHalfGoesToItsEvenNeighbour() throws Exception {
        assertEquals(0, new BigDecimal("2.34").compareTo(rounded("2.345", 2)));
        assertEquals(0, new BigDecimal("2.36").compareTo(rounded("2.355", 2)));
        assertEquals(0, new BigDecimal("-2.34").compareTo(rounded("-2.345", 2)));
        assertEquals(0, new BigDecimal("2.35").compareTo(rounded("2.3451", 2)));
        assertEquals(0, new BigDecimal("2").compareTo(rounded("2.5", 0)));
    }

    @Test
    void aWideDecimalStaysExact() throws Exception {
        assertEquals(0, new BigDecimal("1524157887653619.90").compareTo(rounded("1524157887653619.897366", 2)));
        assertEquals(0, new BigDecimal("1524157887653619.89").compareTo(rounded("1524157887653619.890000", 2)));
    }
}
