// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.database.Databases;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A NEGATION NEVER WRITES {@code --} (B3 step 1b's review, 2026-11-24; step 3's review L3 widened it past DuckDB): a
 * minus written before an operand whose own text starts with a minus would open a SQL comment and silently drop the
 * rest of the line. Every dialect that writes the shared negation parenthesizes such an operand -- a negative literal,
 * a negation, a cast written as its value alone -- and writes any other operand bare.
 */
class NegationLeadingMinusTest {

    private static final List<DatabaseType> DIALECTS = List.of(DatabaseType.DuckDB, DatabaseType.Postgres,
            DatabaseType.H2, DatabaseType.Trino, DatabaseType.Snowflake, DatabaseType.Databricks,
            DatabaseType.ClickHouse);

    private static String render(DatabaseType type, SqlExpr e) {
        SqlSelect select = SqlSelect.starOf(new SqlSource.Table("T", "t0", List.of(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE))
                .withProjections(List.of(new SqlSelect.Projection(e, null, null)));
        return Databases.dialect(type).render(select);
    }

    @Test
    void aMinusLedOperandIsParenthesizedOnEveryDialect() {
        List<SqlExpr> minusLed = List.of(new SqlExpr.IntLit(-2),
                SqlExpr.FloatLit.of(-2.5, SqlExpr.FloatLit.Kind.DOUBLE),
                SqlExpr.Call.of(SqlFn.NEGATE, new SqlExpr.IntLit(5)));
        for (DatabaseType type : DIALECTS) {
            for (SqlExpr x : minusLed) {
                String sql = render(type, SqlExpr.Call.of(SqlFn.NEGATE, x));
                assertFalse(sql.contains("--"), type + " wrote a comment: " + sql);
                assertTrue(sql.contains("-("), type + " parenthesizes " + x + ": " + sql);
            }
        }
    }
}
