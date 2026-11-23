// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies SQL rendering of {@link SqlFn#ERF} across dialects:
 * native {@code erf(...)} on Postgres 16+ and ClickHouse, one-call {@code normal_cdf} on Trino,
 * inline Sun {@code fdlibm}/Cody expansion on DuckDB for single-level expressions with scalar-subquery
 * binding on nested {@code erf}, scalar-subquery binding on SQLite and BigQuery, and simple-only
 * expansion with compound-argument refusal elsewhere.
 */
class ErfSqlTest {

    private static String renderSelect(AnsiSqlRenderer dialect, SqlExpr expr) {
        SqlSelect select = new SqlSelect(
                List.of(new SqlSelect.Projection(expr, "v", new OutputCol("v", SqlType.Scalar.DOUBLE, true))),
                false,
                new SqlSource.Dual(),
                null, List.of(), null, null, List.of(), null, null, List.of());
        return dialect.render(select);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    private static SqlExpr qualifiedCol() {
        return SqlExpr.Column.of("t", "x", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
    }

    private static SqlExpr compoundExpr() {
        return SqlExpr.Call.of(SqlFn.PLUS, qualifiedCol(), new SqlExpr.FloatLit(12.345));
    }

    @Test
    void postgresUsesNativeErfOnce() {
        String sql = renderSelect(new Postgres(), SqlExpr.Call.of(SqlFn.ERF, compoundExpr()));
        assertTrue(sql.contains("erf("), "Expected native erf() in Postgres SQL: " + sql);
        assertEquals(1, countOccurrences(sql, "12.345"), "Expected 1 argument copy in Postgres: " + sql);
        assertFalse(sql.contains("0.46875"), "Postgres should not expand Cody polynomial: " + sql);
    }

    @Test
    void clickHouseUsesNativeErfOnce() {
        String sql = renderSelect(new ClickHouse(), SqlExpr.Call.of(SqlFn.ERF, compoundExpr()));
        assertTrue(sql.contains("erf("), "Expected native erf() in ClickHouse SQL: " + sql);
        assertEquals(1, countOccurrences(sql, "12.345"), "Expected 1 argument copy in ClickHouse: " + sql);
        assertFalse(sql.contains("0.46875"), "ClickHouse should not expand Cody polynomial: " + sql);
    }

    @Test
    void trinoUsesNormalCdfOnce() {
        String sql = renderSelect(new Trino(), SqlExpr.Call.of(SqlFn.ERF, compoundExpr()));
        assertTrue(sql.contains("normal_cdf(0, 0.7071067811865475,"),
                "Expected normal_cdf(0, 0.7071067811865475, ...) in Trino SQL: " + sql);
        assertEquals(1, countOccurrences(sql, "12.345"), "Expected 1 argument copy in Trino: " + sql);
        assertFalse(sql.contains("0.46875"), "Trino should not expand Cody polynomial: " + sql);
    }

    @Test
    void duckDbExpandsSingleLevelInlineWithoutListLambda() {
        String sql = renderSelect(new DuckDb(), SqlExpr.Call.of(SqlFn.ERF, compoundExpr()));
        assertFalse(sql.contains("list_transform("), "DuckDB should not use list_transform: " + sql);
        assertTrue(sql.contains("0.46875"), "Expected fdlibm/Cody expansion in DuckDB SQL: " + sql);
        assertFalse(sql.contains("pi()"), "Should use 1/sqrt(pi) constant instead of pi(): " + sql);
        assertEquals(75, countOccurrences(sql, "12.345"), "Expected 75 inline copies (incl. NaN guard) in DuckDB: " + sql);
    }

    @Test
    void sqliteAndBigQueryBindCompoundArgOnceAndRefuseUnqualified() {
        for (AnsiSqlRenderer dialect : List.of(new SQLite(), new BigQuery())) {
            String sql = renderSelect(dialect, SqlExpr.Call.of(SqlFn.ERF, compoundExpr()));
            assertEquals(1, countOccurrences(sql, "12.345"),
                    "Compound arg should be evaluated once in scalar subquery on " + dialect.getClass().getSimpleName());
            SqlExpr unqualified = SqlExpr.Call.of(SqlFn.PLUS,
                    SqlExpr.Column.of(null, "ux", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED),
                    new SqlExpr.FloatLit(12.345));
            DialectCapability ex = assertThrows(DialectCapability.class,
                    () -> renderSelect(dialect, SqlExpr.Call.of(SqlFn.ERF, unqualified)));
            assertTrue(ex.getMessage().contains("subquery column") && ex.getMessage().contains("distinct()"), ex.getMessage());
        }
    }

    @Test
    void otherDialectsExpandSimpleAndRefuseCompoundByName() {
        for (AnsiSqlRenderer dialect : List.of(
                new H2(), new MySql(), new MariaDb(), new Snowflake(), new Databricks(), new MongoDb())) {
            String simpleSql = renderSelect(dialect, SqlExpr.Call.of(SqlFn.ERF, new SqlExpr.FloatLit(0.5)));
            assertTrue(simpleSql.contains("0.46875"),
                    dialect.getClass().getSimpleName() + " should expand simple literal: " + simpleSql);
            assertFalse(simpleSql.contains("erf("),
                    dialect.getClass().getSimpleName() + " should not emit unverified native erf()");

            String colSql = renderSelect(dialect, SqlExpr.Call.of(SqlFn.ERF, qualifiedCol()));
            assertTrue(colSql.contains("0.46875"),
                    dialect.getClass().getSimpleName() + " should expand simple column");

            DialectCapability ex = assertThrows(DialectCapability.class,
                    () -> renderSelect(dialect, SqlExpr.Call.of(SqlFn.ERF, compoundExpr())),
                    dialect.getClass().getSimpleName() + " must refuse compound erf() argument");
            assertTrue(ex.getMessage().contains("subquery column") && ex.getMessage().contains("distinct()"), ex.getMessage());
        }
    }

    @Test
    void nestedDepthThreeErfGrowsLinearlyAcrossAllSupportedCompoundDialects() {
        SqlExpr depth1 = SqlExpr.Call.of(SqlFn.ERF, compoundExpr());
        SqlExpr depth2 = SqlExpr.Call.of(SqlFn.ERF,
                SqlExpr.Call.of(SqlFn.PLUS, depth1, new SqlExpr.FloatLit(0.25)));
        SqlExpr depth3 = SqlExpr.Call.of(SqlFn.ERF,
                SqlExpr.Call.of(SqlFn.PLUS, depth2, new SqlExpr.FloatLit(0.5)));

        for (AnsiSqlRenderer dialect : List.of(
                new Postgres(), new ClickHouse(), new Trino(), new DuckDb(), new SQLite(), new BigQuery())) {
            String sql1 = renderSelect(dialect, depth1);
            String sql3 = renderSelect(dialect, depth3);
            String unquoted = sql3.replace("\"", "").replace("`", "");
            int copies = countOccurrences(unquoted, "t.x");
            int expectedMaxCopies = dialect instanceof DuckDb ? 75 : 1;
            assertTrue(copies >= 1 && copies <= expectedMaxCopies,
                    "Depth-3 nested erf should keep <=" + expectedMaxCopies + " copies of t.x on "
                            + dialect.getClass().getSimpleName() + ", got " + copies);
            assertTrue(sql3.length() < 4 * Math.max(sql1.length(), 256),
                    "Depth-3 nested erf SQL must stay O(depth) on " + dialect.getClass().getSimpleName()
                            + " (sql1=" + sql1.length() + " B, sql3=" + sql3.length() + " B)");
        }
    }

    @Test
    void unloweredErfInDialectArmThrows() {
        assertThrows(IllegalStateException.class,
                () -> new H2().call(null, SqlExpr.Call.of(SqlFn.ERF, new SqlExpr.FloatLit(0.5)), 0));
    }
}
