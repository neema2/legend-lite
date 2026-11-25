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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A null test as a comparison's operand is parenthesized on every database (2026-11-24): each dialect's own switch
 * passes the precedence it holds to {@link AnsiSqlRenderer#isNullTest}, so each is held here to its text. Written
 * bare, {@code a = b IS NULL} is {@code (a = b) IS NULL} where {@code IS} binds looser than a comparison or beside it
 * (wrong rows with no error, measured on DuckDB and SQLite), and no statement at all where a predicate takes no
 * second one. {@code DuckDbRenderTest.nullTestParens} holds the contexts, on one database.
 */
class NullTestParensTest {

    private static final List<SqlDialect> DIALECTS = List.of(new DuckDb(), new H2(), new Postgres(), new SQLite(),
            new MariaDb(), new MySql(), new Trino(), new Databricks(), new ClickHouse(), new BigQuery(),
            new Snowflake());

    private static SqlExpr column(String name) {
        return SqlExpr.Column.derived("s", name);
    }

    /** {@code SELECT <e> AS t FROM t AS s}, its names with no quotes of any kind. */
    private static String rendered(SqlDialect dialect, SqlExpr e) {
        return dialect.render(SqlSelect.starOf(new SqlSource.Table("t", "s", List.of(), false, Map.of(),
                        com.legend.sql.NameQuoting.BARE))
                .withProjections(List.of(new SqlSelect.Projection(e, "t", null)))).replaceAll("[\"`]", "");
    }

    @Test
    void aNullTestIsParenthesizedAsAComparisonsOperand() {
        SqlExpr isNull = SqlExpr.Call.of(SqlFn.IS_NULL, column("b"));
        SqlExpr isNotNull = SqlExpr.Call.of(SqlFn.IS_NOT_NULL, column("b"));
        for (SqlDialect dialect : DIALECTS) {
            String name = dialect.getClass().getSimpleName();
            String right = rendered(dialect, SqlExpr.Call.of(SqlFn.EQUAL, column("a"), isNull));
            assertTrue(right.contains("s.a = (s.b IS NULL)"), name + ": " + right);
            // on the left of an ordering too: where IS is beside the comparisons (SQLite), x IS NOT NULL < a would
            // be x IS NOT (NULL < a)
            String left = rendered(dialect, SqlExpr.Call.of(SqlFn.LESS, isNotNull, column("a")));
            assertTrue(left.contains("(s.b IS NOT NULL) < s.a"), name + ": " + left);
        }
    }

    @Test
    void aNullTestStaysBareWhereNothingBindsTighter() {
        SqlExpr isNull = SqlExpr.Call.of(SqlFn.IS_NULL, column("b"));
        for (SqlDialect dialect : DIALECTS) {
            String name = dialect.getClass().getSimpleName();
            String whole = rendered(dialect, isNull);
            assertTrue(whole.contains("s.b IS NULL") && !whole.contains("(s.b IS NULL)"), name + ": " + whole);
            String and = rendered(dialect, SqlExpr.Call.of(SqlFn.AND, isNull, SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                    column("a"))));
            assertTrue(and.contains("s.b IS NULL AND s.a IS NOT NULL"), name + ": " + and);
        }
    }
}
