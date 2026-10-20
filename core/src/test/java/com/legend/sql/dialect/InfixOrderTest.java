// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A trailing operand of {@code + - *} of the same precedence keeps its parentheses (IQ island, 2026-10-17, the
 * differential fuzzer's fifth round): {@code w + (2000 - 2020)} written {@code w + 2000 - 2020} overflowed a BIGINT
 * whose sum fits, on DuckDB; a run's own operands stay as written.
 */
class InfixOrderTest {

    private static final long BIGINT_MAX = Long.MAX_VALUE;

    private static SqlSelect projected(SqlExpr e) {
        return SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(new SqlSelect.Projection(e, "a", null)));
    }

    @Test
    void aTrailingSumOrDifferenceKeepsItsParentheses() {
        SqlExpr w = new SqlExpr.IntLit(BIGINT_MAX);
        SqlExpr inner = SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(2000), new SqlExpr.IntLit(2020));
        String sql = new DuckDb().render(projected(SqlExpr.Call.of(SqlFn.PLUS, w, inner)));
        assertTrue(sql.contains(BIGINT_MAX + " + (2000 - 2020)"), sql);
        String product = new DuckDb().render(projected(SqlExpr.Call.of(SqlFn.TIMES, new SqlExpr.IntLit(2),
                SqlExpr.Call.of(SqlFn.TIMES, new SqlExpr.IntLit(3), new SqlExpr.IntLit(4)))));
        assertTrue(product.contains("2 * (3 * 4)"), product);
        String leading = new DuckDb().render(projected(SqlExpr.Call.of(SqlFn.PLUS, inner, new SqlExpr.IntLit(1))));
        assertTrue(leading.contains("2000 - 2020 + 1"), leading);
    }

    @Test
    void theSumFitsOnDuckDb() throws Exception {
        SqlExpr sum = SqlExpr.Call.of(SqlFn.PLUS, new SqlExpr.IntLit(BIGINT_MAX), SqlExpr.Call.of(SqlFn.MINUS,
                new SqlExpr.IntLit(2000), new SqlExpr.IntLit(2020)));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(new DuckDb().render(projected(sum)))) {
            assertTrue(rs.next());
            assertEquals(BIGINT_MAX - 20, rs.getLong(1));
        }
    }
}
