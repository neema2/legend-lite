// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A window frame offset the database computes ({@code ComputedPreceding}; docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md
 * §3a) as each dialect writes it: where the database was measured to take an expression (DuckDB, H2, SQLite, Postgres,
 * Trino, ClickHouse) the offset parenthesized, inside the check the database works out (a start after the end, a
 * negative offset); MariaDB and MySQL, which take none, refuse it by name, and so do the dialects not yet measured
 * (BigQuery, Databricks, Snowflake) and ClickHouse's RANGE frames.
 */
class ComputedFrameOffsetTest {

    private static final SqlSource.Table T = new SqlSource.Table("T", "t0", List.of(), false, java.util.Map.of(),
            com.legend.sql.NameQuoting.BARE);

    private static SqlSelect select(SqlExpr.WindowCall.Frame.Kind kind) {
        SqlExpr v = new SqlExpr.Column("t0", "V", new com.legend.sql.TypeFact.Typed(com.legend.sql.SqlType.Scalar.BIGINT), com.legend.sql.OutputCol.Origin.PHYSICAL);
        SqlExpr offset = SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(3), new SqlExpr.IntLit(1));
        SqlExpr.WindowCall w = new SqlExpr.WindowCall(SqlAgg.Reducer.of(SqlAgg.Fn.SUM, v),
                List.of(SqlExpr.Column.physical("t0", "P")), List.of(SqlSelect.SortKey.asc(SqlExpr.Column.physical("t0", "O"))),
                new SqlExpr.WindowCall.Frame(kind, new SqlExpr.WindowCall.Frame.Bound.ComputedPreceding(offset),
                        new SqlExpr.WindowCall.Frame.Bound.CurrentRow()));
        return SqlSelect.starOf(T).withProjections(List.of(new SqlSelect.Projection(w, "s", null)));
    }

    @Test
    @DisplayName("the databases measured to take an expression: (offset) PRECEDING inside the check")
    void writtenInsideItsCheck() {
        for (DatabaseType t : List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite, DatabaseType.Postgres,
                DatabaseType.Trino, DatabaseType.ClickHouse)) {
            String sql = Databases.dialect(t).render(select(SqlExpr.WindowCall.Frame.Kind.ROWS));
            // the offset as the dialect spells the arithmetic (Trino: BIGINT '3' - BIGINT '1'; ClickHouse: toInt64(3)
            // - toInt64(1)), parenthesized
            assertTrue(sql.matches("(?s).*ROWS BETWEEN \\(.*3.* - .*1.*\\) PRECEDING AND CURRENT ROW.*"),
                    t + ": " + sql);
            assertTrue(sql.contains("CASE WHEN"), t + ": the check: " + sql);
            assertTrue(sql.contains(FrameGuard.ORDER) && sql.contains("S129"), t + ": both refusals: " + sql);
        }
    }

    @Test
    @DisplayName("MariaDB, MySQL and the dialects not yet measured refuse a computed offset by name")
    void refusedByName() {
        for (DatabaseType t : List.of(DatabaseType.MariaDB, DatabaseType.MySQL, DatabaseType.BigQuery,
                DatabaseType.Databricks, DatabaseType.Snowflake)) {
            DialectCapability e = assertThrows(DialectCapability.class,
                    () -> Databases.dialect(t).render(select(SqlExpr.WindowCall.Frame.Kind.ROWS)), t.toString());
            assertTrue(e.getMessage().contains("computes"), t + ": " + e.getMessage());
        }
        DialectCapability e = assertThrows(DialectCapability.class,
                () -> Databases.dialect(DatabaseType.ClickHouse).render(select(SqlExpr.WindowCall.Frame.Kind.RANGE)));
        assertTrue(e.getMessage().contains("RANGE"), e.getMessage());
    }
}
