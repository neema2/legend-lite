// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SqlFn#BINARY_COLLATE}, a text ordered by its code points whatever its column's or its database's collation (the
 * test-data generator's seed: the design's §3.1, ruled with R4; the B2d.3 audit's M4, 2026-11-12): every dialect
 * spells it or refuses it by name. Postgres writes {@code COLLATE "C"} and SQLite {@code COLLATE BINARY}; DuckDB and
 * H2 (and legend-engine's H2 text) compare texts by their code points already (measured 2026-11-12: {@code B E Z a b
 * é}), so the text as it is; a dialect whose binary collation is not measured refuses it. What it means on Postgres is
 * {@code PostgresArmTest.theSeedOrdersATextKeyByItsCodePoints}'s, on a column collated otherwise.
 */
class BinaryCollateTest {

    /** {@code SELECT 'b' AS v ORDER BY <BINARY_COLLATE('b')>}. */
    private static SqlSelect ordered() {
        SqlExpr text = new SqlExpr.StringLit("b");
        return new SqlSelect(List.of(new SqlSelect.Projection(text, "v", null)), false, new SqlSource.Dual(), null,
                List.of(), null, null, List.of(SqlSelect.SortKey.asc(SqlExpr.Call.of(SqlFn.BINARY_COLLATE, text))),
                null, null, List.of());
    }

    @Test
    void eachDatabaseWithAMeasuredBinaryCollationSpellsIt() {
        Map<SqlDialect, String> spelled = Map.of(
                new Postgres(), "('b' COLLATE \"C\")",
                new SQLite(), "('b' COLLATE BINARY)");
        spelled.forEach((dialect, key) -> {
            String sql = dialect.render(ordered());
            assertTrue(sql.contains("ORDER BY " + key), dialect.getClass().getSimpleName() + ": " + sql);
        });
        // binary by default: the text as it is
        for (SqlDialect dialect : List.of(new DuckDb(), new H2(), new EngineStyleH2())) {
            String sql = dialect.render(ordered());
            assertTrue(sql.matches("(?is).*order by 'b'.*") && !sql.toUpperCase(java.util.Locale.ROOT).contains(
                    "COLLATE"), dialect.getClass().getSimpleName() + ": " + sql);
        }
    }

    @Test
    void aDatabaseWhoseBinaryCollationIsNotMeasuredRefusesItByName() {
        Map<SqlDialect, String> refused = Map.of(
                new MariaDb(), "MariaDB",
                new MySql(), "MariaDB",
                new Trino(), "Trino",
                new ClickHouse(), "ClickHouse",
                new Snowflake(), "Snowflake",
                new Databricks(), "Databricks",
                new BigQuery(), "BigQuery",
                new EngineStyleDB2(), "legend-engine's DB2 text",
                new EngineStyleComposite(), "legend-engine's DB2 text");
        refused.forEach((dialect, named) -> {
            DialectCapability refusal = assertThrows(DialectCapability.class, () -> dialect.render(ordered()),
                    dialect.getClass().getSimpleName());
            assertEquals("a binary collation (texts ordered by their code points) reached " + named + ", whose binary"
                    + " collation is not measured", refusal.getMessage());
        });
    }
}
