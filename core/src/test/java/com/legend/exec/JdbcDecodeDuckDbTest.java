// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The typed seam's decode ({@link JdbcDecodeCases}) on DuckDB's driver. */
class JdbcDecodeDuckDbTest {

    private static final com.legend.model.ConnectionDefinition.DatabaseType DUCKDB =
            com.legend.model.ConnectionDefinition.DatabaseType.DuckDB;

    @Test
    void theSlotDecidesTheCarrier_onDuckDb() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            List<Object> r = JdbcDecodeCases.row(c, DUCKDB, "SELECT 7::BIGINT AS i, 'seven' AS s, 1.5::DOUBLE AS f, 2.50::DECIMAL(10,2) AS d,"
                    + " DATE '2024-01-02' AS day, TIMESTAMP '2024-01-02 10:30:00.123' AS ts, TIME '10:30:00' AS t,"
                    + " true AS b, NULL::INTEGER AS n, [1, 2]::BIGINT[] AS l, {'name': 'a', 'age': 1} AS st,"
                    + " '{\"k\": 1}'::JSON AS j, 170141183460469231731687303715884105727::HUGEINT AS h",
                    List.of(JdbcDecodeCases.col("i", "Integer", "BIGINT"), JdbcDecodeCases.col("s", "String", "VARCHAR"), JdbcDecodeCases.col("f", "Float", "DOUBLE"),
                            JdbcDecodeCases.col("d", "Decimal", "DECIMAL(10,2)"), JdbcDecodeCases.col("day", "StrictDate", "DATE"),
                            JdbcDecodeCases.col("ts", "DateTime", "TIMESTAMP"), JdbcDecodeCases.col("t", "Time", "TIME"), JdbcDecodeCases.col("b", "Boolean", "BOOLEAN"),
                            JdbcDecodeCases.opt("n", "Integer", "INTEGER"), JdbcDecodeCases.many("l", "Integer", "ARRAY<BIGINT>"),
                            JdbcDecodeCases.col("st", "t::P", "STRUCT<name VARCHAR, age INTEGER>"),
                            JdbcDecodeCases.col("j", "meta::pure::metamodel::variant::Variant", "JSON"),
                            JdbcDecodeCases.col("h", "Integer", "HUGEINT")));
            assertEquals(7L, r.get(0));
            assertEquals("seven", r.get(1));
            assertEquals(1.5d, r.get(2));
            assertEquals(new BigDecimal("2.50"), r.get(3));
            assertEquals(PureDateLiteral.parse("2024-01-02"), r.get(4));
            // a timestamp as the engine reads one: nine digits (PureDateLiteral.fromDatabaseTimestamp)
            assertEquals(PureDateLiteral.parse("2024-01-02T10:30:00.123000000"), r.get(5));
            assertEquals(PureTimeLiteral.parse("10:30:00"), r.get(6));
            assertEquals(true, r.get(7));
            assertNull(r.get(8));
            assertEquals(List.of(1L, 2L), r.get(9));
            assertEquals(Map.of("name", "a", "age", 1L), r.get(10));
            assertEquals("{\"k\":1}", r.get(11));
            assertEquals(new BigInteger("170141183460469231731687303715884105727"), r.get(12));
        }
    }

    /** A text the database holds under a numeric slot is the column's own text unless the Pure type is numeric (the
     *  engine's test store declares an INT over a VARCHAR column; step 6's finding), and a Float the store holds as
     *  an integer is the integer the database holds. */
    @Test
    void aTextUnderANumericSlotIsTheTextUnlessThePureTypeIsNumeric() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            assertEquals("7", JdbcDecodeCases.row(c, DUCKDB, "SELECT '7' AS id", List.of(JdbcDecodeCases.col("id", "String", "INTEGER"))).get(0));
            assertEquals(7L, JdbcDecodeCases.row(c, DUCKDB, "SELECT '7' AS id", List.of(JdbcDecodeCases.col("id", "Integer", "INTEGER"))).get(0));
            assertEquals(1000000L, JdbcDecodeCases.row(c, DUCKDB, "SELECT 1000000::INTEGER AS q", List.of(JdbcDecodeCases.col("q", "Float", "INTEGER"))).get(0));
            // a number under a text slot keeps its digits
            assertEquals("2.50", JdbcDecodeCases.row(c, DUCKDB, "SELECT 2.50::DECIMAL(10,2) AS d", List.of(JdbcDecodeCases.col("d", "String", "VARCHAR"))).get(0));
        }
    }

    /** No slot (a late-bound grid's column): the Pure type decides, and a class or Any takes the driver's own kind. */
    @Test
    void withoutASlotThePureTypeDecides() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            List<Object> r = JdbcDecodeCases.row(c, DUCKDB, "SELECT 7 AS i, 1.5 AS f, 'x' AS s, DATE '2024-01-02' AS d, 3 AS any",
                    List.of(JdbcDecodeCases.col("i", "Integer", null), JdbcDecodeCases.col("f", "Float", null), JdbcDecodeCases.col("s", "String", null),
                            JdbcDecodeCases.col("d", "Date", null), JdbcDecodeCases.col("any", "meta::pure::metamodel::type::Any", null)));
            assertEquals(7L, r.get(0));
            assertEquals(1.5d, r.get(1));
            assertEquals("x", r.get(2));
            assertEquals(PureDateLiteral.parse("2024-01-02"), r.get(3));
            assertEquals(7L, r.get(4) instanceof Long ? 7L : r.get(4));
            assertEquals(3L, r.get(4));
        }
    }

    /** A cell no rule reads is refused by name, never guessed; a statement answering other columns than the plan's
     *  is refused before a row is read. */
    @Test
    void whatNoRuleReadsIsRefusedByName() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            String refused = assertThrows(IllegalStateException.class,
                    () -> JdbcDecodeCases.row(c, DUCKDB, "SELECT 'x' AS b", List.of(JdbcDecodeCases.col("b", "Boolean", "BOOLEAN")))).getMessage();
            assertTrue(refused.startsWith("column 'b' is declared Boolean in slot BOOLEAN, and the driver's value, a"
                    + " java.lang.String x, is not of that kind"), refused);
            String columns = assertThrows(IllegalStateException.class,
                    () -> JdbcDecodeCases.row(c, DUCKDB, "SELECT 1 AS a, 2 AS b", List.of(JdbcDecodeCases.col("a", "Integer", "INTEGER")))).getMessage();
            assertEquals("the statement answers 2 columns, and the plan declares 1 [a]: a typed-rows statement"
                    + " answers the plan's columns in their order", columns);
        }
    }
}
