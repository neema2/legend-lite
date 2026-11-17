// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SqlFn#CONCAT_WS}, a row's cells joined with an empty cell skipped (Phase 3 of
 * docs/records/total-wall-edge-program-2026-11-16.md): where a database has no NULL-skipping CONCAT_WS the parts go
 * into an array whose NULLs are dropped, so a part must reach the array AS IT IS -- a part coalesced to {@code ''} first
 * is no NULL to drop (lane round 75, 2026-11-17: ClickHouse answered {@code 'a, , c'} and {@code ', , '}). Each array
 * form is held here to its text, so this cannot come back without a lane round; the native forms are held to one
 * CONCAT_WS with no per-part coalesce.
 */
class ConcatWsRenderTest {

    /** {@code SELECT CONCAT_WS(', ', s.a, s.b, s.c) AS t FROM t AS s}. */
    private static SqlSelect joined() {
        // each part a cell's text, as the lowering writes it: a cast of the cell to text
        SqlExpr join = new SqlExpr.Call(SqlFn.CONCAT_WS, List.of(new SqlExpr.StringLit(", "), text("a"), text("b"),
                text("c")));
        return SqlSelect.starOf(new SqlSource.Table("t", "s", List.of(), false, Map.of(),
                        com.legend.sql.NameQuoting.BARE))
                .withProjections(List.of(new SqlSelect.Projection(join, "t", null)));
    }

    private static SqlExpr text(String column) {
        return new SqlExpr.Cast(SqlExpr.Column.derived("s", column), com.legend.sql.SqlType.Scalar.VARCHAR,
                SqlExpr.Reason.PRINT);
    }

    /** A part already a text is cast no second time (round 75's lean note, 2026-11-17: SQLite, MariaDB, MySQL and
     *  BigQuery wrote CAST(CAST(x AS TEXT) AS TEXT)). */
    private static void castOnce(String database, String sql) {
        assertFalse(sql.toLowerCase(java.util.Locale.ROOT).replace(" ", "").contains("cast(cast("),
                database + ": a part cast twice: " + sql);
    }

    /** The text between the array's brackets in {@code sql}, which opens at {@code opener}. */
    private static String arrayOf(String sql, String opener) {
        int at = sql.indexOf(opener);
        assertTrue(at >= 0, opener + " in " + sql);
        int start = at + opener.length();
        int depth = 1;
        for (int i = start; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '[' || ch == '(') {
                depth++;
            } else if (ch == ']' || ch == ')') {
                depth--;
                if (depth == 0) {
                    return sql.substring(start, i);
                }
            }
        }
        throw new AssertionError("unclosed array in " + sql);
    }

    private static void eachPartAsItIs(String database, String array) {
        assertFalse(array.toLowerCase(java.util.Locale.ROOT).matches("(?s).*(coalesce|ifnull|nvl)\\s*\\(.*"),
                database + ": a part is coalesced inside the array, so its NULL is no longer dropped: " + array);
        Matcher parts = Pattern.compile("\\b[ABCabc]\\b").matcher(array);
        int n = 0;
        while (parts.find()) {
            n++;
        }
        assertTrue(n == 3, database + ": the three parts in the array: " + array);
    }

    @Test
    void eachArrayFormTakesThePartsAsTheyAre() {
        String clickHouse = new ClickHouse().render(joined());
        eachPartAsItIs("ClickHouse", arrayOf(clickHouse, "\"__x\" IS NOT NULL, ["));
        castOnce("ClickHouse", clickHouse);
        String bigQuery = new BigQuery().render(joined());
        eachPartAsItIs("BigQuery", arrayOf(bigQuery, "ARRAY_TO_STRING(["));
        castOnce("BigQuery", bigQuery);
        String snowflake = new Snowflake().render(joined());
        eachPartAsItIs("Snowflake", arrayOf(snowflake, "ARRAY_CONSTRUCT_COMPACT("));
        castOnce("Snowflake", snowflake);
    }

    @Test
    void eachNativeFormIsOneConcatWsWithNoPartCoalesced() {
        for (SqlDialect dialect : List.of(new DuckDb(), new H2(), new Postgres(), new SQLite(), new MariaDb(),
                new MySql(), new Trino(), new Databricks())) {
            String sql = dialect.render(joined());
            String lower = sql.toLowerCase(java.util.Locale.ROOT);
            assertTrue(lower.contains("concat_ws(', '"), dialect.getClass().getSimpleName() + ": " + sql);
            eachPartAsItIs(dialect.getClass().getSimpleName(), arrayOf(lower, "concat_ws(', ', "));
            castOnce(dialect.getClass().getSimpleName(), sql);
        }
    }
}
