// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.compiler.element.type.Type;
import com.legend.exec.WireTypes;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A driver's own name for a result column's type is read by its dialect ({@link SqlDialect#wireTypeName}) before a
 * pivot-made or late-bound column is typed (the dialect program's Phase 4, 2026-10-15). Defect 2 of the plan
 * (docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md §3): the Pure-type table read the driver's name as given, so
 * Postgres's {@code int8}, ClickHouse's {@code Nullable(Int64)} and BigQuery's {@code INT64} had no type there (a
 * dynamic pivot's column refused, a raw grid's column fell back to Any).
 */
class WireTypeNamesTest {

    private static @com.legend.base.Nullable Type typed(SqlDialect dialect, String driverName) {
        String wire = dialect.typingWireName(driverName);
        return wire == null ? null : com.legend.driver.WireProbe.pureOfSqlTypeOrNull(wire);
    }

    /** The three names the defect named: none has a type as the driver gives it, each has one through its dialect. */
    @Test
    void theDefectsThreeNamesAreTypedThroughTheirDialects() {
        for (String name : new String[] {"int8", "Nullable(Int64)", "INT64"}) {
            assertNull(WireTypes.pureTypeNameOrNull(name), name + " as the driver gives it");
        }
        assertEquals(Type.Primitive.INTEGER, typed(new com.legend.sql.dialect.Postgres(), "int8"));
        assertEquals(Type.Primitive.INTEGER, typed(new com.legend.sql.dialect.ClickHouse(), "Nullable(Int64)"));
        assertEquals(Type.Primitive.INTEGER, typed(new com.legend.sql.dialect.BigQuery(), "INT64"));
    }

    /** Each database's own names for an integer, a float, a string, a boolean, a date and a date-time. */
    @Test
    void eachDatabasesOwnNamesAreTyped() {
        Map<SqlDialect, Map<String, Type>> names = Map.ofEntries(
                Map.entry(new com.legend.sql.dialect.DuckDb(), Map.of("BIGINT", Type.Primitive.INTEGER,
                        "HUGEINT", Type.Primitive.INTEGER, "DOUBLE", Type.Primitive.FLOAT,
                        "VARCHAR", Type.Primitive.STRING, "BOOLEAN", Type.Primitive.BOOLEAN,
                        "DATE", Type.Primitive.STRICT_DATE, "TIMESTAMP", Type.Primitive.DATE_TIME,
                        "DECIMAL(18,3)", Type.Primitive.DECIMAL)),
                Map.entry(new com.legend.sql.dialect.H2(), Map.of("BIGINT", Type.Primitive.INTEGER,
                        "CHARACTER VARYING", Type.Primitive.STRING, "BOOLEAN", Type.Primitive.BOOLEAN,
                        "TIMESTAMP", Type.Primitive.DATE_TIME)),
                Map.entry(new com.legend.sql.dialect.Postgres(), Map.of("int8", Type.Primitive.INTEGER,
                        "int4", Type.Primitive.INTEGER, "float8", Type.Primitive.FLOAT,
                        "text", Type.Primitive.STRING, "bool", Type.Primitive.BOOLEAN,
                        "date", Type.Primitive.STRICT_DATE, "timestamp", Type.Primitive.DATE_TIME,
                        "numeric", Type.Primitive.DECIMAL)),
                Map.entry(new com.legend.sql.dialect.SQLite(), Map.of("INTEGER", Type.Primitive.INTEGER,
                        "FLOAT", Type.Primitive.FLOAT, "TEXT", Type.Primitive.STRING)),
                Map.entry(new com.legend.sql.dialect.MariaDb(), Map.of("BIGINT", Type.Primitive.INTEGER,
                        "LONGTEXT", Type.Primitive.STRING, "DATETIME", Type.Primitive.DATE_TIME)),
                Map.entry(new com.legend.sql.dialect.MySql(), Map.of("INT", Type.Primitive.INTEGER,
                        "TEXT", Type.Primitive.STRING, "DATETIME", Type.Primitive.DATE_TIME)),
                Map.entry(new com.legend.sql.dialect.Trino(), Map.of("bigint", Type.Primitive.INTEGER,
                        "double", Type.Primitive.FLOAT, "varchar", Type.Primitive.STRING,
                        "timestamp(6)", Type.Primitive.DATE_TIME, "decimal(10,2)", Type.Primitive.DECIMAL)),
                Map.entry(new com.legend.sql.dialect.ClickHouse(), Map.of("Int64", Type.Primitive.INTEGER,
                        "Nullable(Int32)", Type.Primitive.INTEGER, "Float64", Type.Primitive.FLOAT,
                        "String", Type.Primitive.STRING, "Bool", Type.Primitive.BOOLEAN,
                        "Date32", Type.Primitive.STRICT_DATE, "DateTime64(6, 'UTC')", Type.Primitive.DATE_TIME,
                        "Nullable(Decimal(10, 2))", Type.Primitive.DECIMAL)),
                Map.entry(new com.legend.sql.dialect.Snowflake(), Map.of(
                        "FLOAT", Type.Primitive.FLOAT, "VARCHAR", Type.Primitive.STRING, "BOOLEAN", Type.Primitive.BOOLEAN,
                        "TIMESTAMP_NTZ", Type.Primitive.DATE_TIME)),
                Map.entry(new com.legend.sql.dialect.Databricks(), Map.of("INT", Type.Primitive.INTEGER,
                        "BIGINT", Type.Primitive.INTEGER, "STRING", Type.Primitive.STRING,
                        "TIMESTAMP", Type.Primitive.DATE_TIME)),
                Map.entry(new com.legend.sql.dialect.BigQuery(), Map.of("INT64", Type.Primitive.INTEGER,
                        "FLOAT64", Type.Primitive.FLOAT, "STRING", Type.Primitive.STRING,
                        "BOOL", Type.Primitive.BOOLEAN, "DATE", Type.Primitive.STRICT_DATE,
                        "DATETIME", Type.Primitive.DATE_TIME)));
        names.forEach((dialect, byName) -> byName.forEach((name, type) ->
                assertEquals(type, typed(dialect, name), dialect.getClass().getSimpleName() + " " + name)));
    }

    /** A name is the database's own: DuckDB has no DATETIME, so its dialect gives the name no type. */
    @Test
    void aNameIsReadAsItsOwnDatabaseNamesIt() {
        assertNull(typed(new com.legend.sql.dialect.DuckDb(), "DATETIME"));
        assertEquals(Type.Primitive.DATE_TIME, typed(new com.legend.sql.dialect.MariaDb(), "DATETIME"));
        // SQLite names a NULL value's column NUMERIC: no type, where a decimal's NUMERIC elsewhere is DECIMAL
        assertNull(typed(new com.legend.sql.dialect.SQLite(), "NUMERIC"));
        assertEquals(Type.Primitive.DECIMAL, typed(new com.legend.sql.dialect.Postgres(), "NUMERIC"));
    }

    /** A name that does not say its type alone types nothing (2026-10-15, the audit of Phase 4): Snowflake's NUMBER is
     *  an integer or a decimal by its scale, so a column it names is not typed by the name, where the census still
     *  reads it as a decimal its precision and scale size. The driver's own name, which the PCT probe keeps for such a
     *  column, types nothing either. */
    @Test
    void aNameThatDoesNotSayItsTypeTypesNothing() {
        SqlDialect snowflake = new com.legend.sql.dialect.Snowflake();
        assertNull(snowflake.typingWireName("NUMBER"));
        assertNull(WireTypes.pureTypeNameOrNull("NUMBER"));
        assertEquals("DECIMAL", snowflake.wireTypeName("NUMBER"));
        // no other database's driver gives the name, so the names every driver's reading folds do not hold it
        assertEquals("NUMBER", WireNames.fold("NUMBER"));
    }

    /** A probed column its dialect does not type by name is absent from the probe's type names, and the lookup refuses
     *  it: no fallback to the driver's own name (SQLite's NUMERIC would type Decimal) or to the probe's slot. */
    @Test
    void aProbedColumnWithNoTypeIsRefusedNotGuessed() {
        var probe = new com.legend.sql.PlanProbe(java.util.List.of(), Map.of("a", "BIGINT"));
        assertEquals("BIGINT", probe.typeName("a"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> probe.typeName("n"));
    }
}
