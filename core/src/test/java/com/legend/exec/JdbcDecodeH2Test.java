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

/** The typed seam's decode ({@link JdbcDecodeCases}) on H2's driver. */
class JdbcDecodeH2Test {

    private static final com.legend.model.ConnectionDefinition.DatabaseType H2 =
            com.legend.model.ConnectionDefinition.DatabaseType.H2;

    @Test
    void theSlotDecidesTheCarrier_onH2() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:jdbcdecode" + com.legend.testbackend.TestBackends.h2UrlSettings())) {
            List<Object> r = JdbcDecodeCases.row(c, H2, "SELECT CAST(7 AS BIGINT) AS I, 'seven' AS S, CAST(1.5 AS DOUBLE) AS F,"
                    + " CAST(2.50 AS DECIMAL(10,2)) AS D, DATE '2024-01-02' AS DAY,"
                    + " TIMESTAMP '2024-01-02 10:30:00.123' AS TS, TIME '10:30:00' AS T, TRUE AS B,"
                    + " CAST(NULL AS INTEGER) AS N, JSON '{\"k\": 1}' AS J,"
                    + " (SELECT SUM(X) FROM (VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE))) AS V(X)) AS SUMMED",
                    List.of(JdbcDecodeCases.col("I", "Integer", "BIGINT"), JdbcDecodeCases.col("S", "String", "VARCHAR"), JdbcDecodeCases.col("F", "Float", "DOUBLE"),
                            JdbcDecodeCases.col("D", "Decimal", "DECIMAL(10,2)"), JdbcDecodeCases.col("DAY", "StrictDate", "DATE"),
                            JdbcDecodeCases.col("TS", "DateTime", "TIMESTAMP"), JdbcDecodeCases.col("T", "Time", "TIME"), JdbcDecodeCases.col("B", "Boolean", "BOOLEAN"),
                            JdbcDecodeCases.opt("N", "Integer", "INTEGER"), JdbcDecodeCases.col("J", "meta::pure::metamodel::variant::Variant", "JSON"),
                            JdbcDecodeCases.col("SUMMED", "Float", "DOUBLE")));
            assertEquals(7L, r.get(0));
            assertEquals("seven", r.get(1));
            assertEquals(1.5d, r.get(2));
            assertEquals(new BigDecimal("2.50"), r.get(3));
            assertEquals(PureDateLiteral.parse("2024-01-02"), r.get(4));
            assertEquals(PureDateLiteral.parse("2024-01-02T10:30:00.123000000"), r.get(5));
            assertEquals(PureTimeLiteral.parse("10:30:00"), r.get(6));
            assertEquals(true, r.get(7));
            assertNull(r.get(8));
            assertEquals("{\"k\":1}", r.get(9));
            // H2's LEGACY mode hands a DOUBLE aggregate back as a BigDecimal: the slot says the double
            assertEquals(4.0d, r.get(10));
        }
    }

    /** A database whose JSON spells a double that is not finite as a bare token ({@code DatabaseConnector.decoding().nonFiniteJson}) has its Variant read with the token, written back as read; another database's
     *  JSON is JSON alone (2026-10-27: the Cypher TCK's ordered values of every type, NaN among them). */
    @Test
    void aNonFiniteTokenIsReadWhereTheDatabaseSpellsOne() throws Exception {
        ExecutionPlan.Column variant = JdbcDecodeCases.col("V", "meta::pure::metamodel::variant::Variant", "JSON");
        assertEquals("[1.0,NaN,-Infinity]", JdbcDecode.decode("[1.0, NaN, -Infinity]", variant,
                new DatabaseConnector.Decoding(false, false, false, true, true)));
        assertThrows(IllegalArgumentException.class, () -> JdbcDecode.decode("[1.0, NaN]", variant,
                new DatabaseConnector.Decoding(false, false, false, false, true)));
    }

    /** A driver's Timestamp is read as its wall clock with its era: a BC year is negative, never the year of its era
     *  (a driver that re-reads no timestamp as java.time hands a Timestamp, whose toLocalDateTime reads year -21457 as
     *  21458: Databricks' lane, testAdjustByMinutesBigNumber, 2026-10-27); an AD one as it reads. */
    @Test
    void aTimestampKeepsItsEra() throws Exception {
        ExecutionPlan.Column ts = JdbcDecodeCases.col("TS", "DateTime", "TIMESTAMP");
        DatabaseConnector.Decoding driver = new DatabaseConnector.Decoding(false, false, false, false, true);
        java.time.LocalDateTime bc = java.time.LocalDateTime.of(-21457, 1, 8, 20, 48);
        java.sql.Timestamp held = java.sql.Timestamp.valueOf(bc);
        assertEquals(21458, held.toLocalDateTime().getYear(), "the year of the era, as the JDK reads it");
        assertEquals(PureDateLiteral.fromDatabaseTimestamp(bc), JdbcDecode.decode(held, ts, driver));
        // a date's too: 44 BC (PostgresArmTest's make_date(-44, 3, 15), where its driver's re-read keeps it)
        java.time.LocalDate ides = java.time.LocalDate.of(-43, 3, 15);
        assertEquals(PureDateLiteral.fromLocalDate(ides), JdbcDecode.decode(java.sql.Date.valueOf(ides),
                JdbcDecodeCases.col("D", "StrictDate", "DATE"), driver));
        java.time.LocalDateTime ad = java.time.LocalDateTime.of(2016, 2, 29, 9, 37, 37, 400_000_000);
        assertEquals(PureDateLiteral.fromDatabaseTimestamp(ad), JdbcDecode.decode(java.sql.Timestamp.valueOf(ad), ts,
                driver));
    }

    /** A driver that drops a decimal's trailing zeros (BigQuery's, {@code DatabaseConnector.decoding().dropsDecimalZeros}) has
     *  them padded back to the slot's declared scale, never rounded; another driver's decimal is as it answers
     *  (2026-10-27, found by the BigQuery lane: {@code abs(3.0D)} answered {@code 3D}). H2 answers a scale-0 decimal,
     *  standing for the stripped one. */
    @Test
    void aDecimalsDroppedZerosArePaddedToItsSlotsScale() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:jdbcdecode_zeros" + com.legend.testbackend.TestBackends.h2UrlSettings());
                java.sql.Statement st = c.createStatement();
                java.sql.ResultSet rs = st.executeQuery("SELECT CAST(3 AS DECIMAL(10,0)) AS D, CAST(2 AS DECIMAL(10,0)) AS E")) {
            rs.next();
            List<ExecutionPlan.Column> cols = List.of(JdbcDecodeCases.col("D", "Decimal", "DECIMAL(2,1)"),
                    JdbcDecodeCases.col("E", "Decimal", "DECIMAL"));
            List<Object> padded = JdbcDecode.row(rs, cols, new DatabaseConnector.Decoding(false, true, false, false, true));
            assertEquals(new BigDecimal("3.0"), padded.get(0));
            assertEquals(1, ((BigDecimal) padded.get(0)).scale());
            // a slot with no declared scale pads nothing
            assertEquals(new BigDecimal("2"), padded.get(1));
            List<Object> kept = JdbcDecode.row(rs, cols, new DatabaseConnector.Decoding(false, false, false, false, true));
            assertEquals(0, ((BigDecimal) kept.get(0)).scale());
        }
    }

    /** A database whose driver re-reads no timestamp as java.time keeps the driver's Timestamp, the same date-time
     *  for an AD year (2026-10-21: Snowflake's driver refuses the re-read, and the typed seam re-read every timestamp
     *  regardless; the legacy executor's rule, {@code DatabaseConnector.decoding().javaTime}). */
    @Test
    void aTimestampTheDriverDoesNotReReadIsTheDriversOwn() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:jdbcdecode_keep" + com.legend.testbackend.TestBackends.h2UrlSettings());
                java.sql.Statement st = c.createStatement();
                java.sql.ResultSet rs = st.executeQuery("SELECT TIMESTAMP '2024-01-02 10:30:00.123456' AS TS")) {
            rs.next();
            assertTrue(rs.getObject(1) instanceof java.sql.Timestamp, "H2 hands a TIMESTAMP back as a Timestamp");
            List<ExecutionPlan.Column> cols = List.of(JdbcDecodeCases.col("TS", "DateTime", "TIMESTAMP"));
            assertEquals(List.of(PureDateLiteral.parse("2024-01-02T10:30:00.123456000")),
                    JdbcDecode.row(rs, cols, new DatabaseConnector.Decoding(false, false, false, false, true)));
            assertEquals(JdbcDecode.row(rs, cols, new DatabaseConnector.Decoding(true, false, false, false, true)),
                    JdbcDecode.row(rs, cols, new DatabaseConnector.Decoding(false, false, false, false, true)));
        }
    }

    /** A statement's read over many rows (JdbcDecode.Reader, PR #85's review, 2026-11-01): a driver that re-reads no
     *  timestamp (Snowflake's, Databricks') never has a cell read as java.time, on any row; one that does reads a
     *  TIMESTAMP column's cells as java.time at once from its second non-null cell, its nulls still nulls, every
     *  row's value the same either way. The getters the read calls are recorded on H2's own result set. */
    @Test
    void aStatementsReadReadsATimestampAsItsDriverDecides() throws Exception {
        String sql = "SELECT TS FROM (VALUES (1, CAST(NULL AS TIMESTAMP)), (2, TIMESTAMP '2024-01-02 10:30:00.123456'),"
                + " (3, CAST(NULL AS TIMESTAMP)), (4, TIMESTAMP '1999-12-31 23:59:59'),"
                + " (5, TIMESTAMP '2024-06-03 09:07:00.5')) V(K, TS) ORDER BY K";
        List<ExecutionPlan.Column> cols = List.of(JdbcDecodeCases.opt("TS", "DateTime", "TIMESTAMP"));
        List<Object> expected = java.util.Arrays.asList(null, PureDateLiteral.parse("2024-01-02T10:30:00.123456000"),
                null, PureDateLiteral.parse("1999-12-31T23:59:59.000000000"),
                PureDateLiteral.parse("2024-06-03T09:07:00.500000000"));
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:jdbcdecode_reader" + com.legend.testbackend.TestBackends.h2UrlSettings())) {
            for (boolean javaTime : new boolean[] {false, true}) {
                List<String> calls = new java.util.ArrayList<>();
                List<Object> cells = new java.util.ArrayList<>();
                try (java.sql.Statement st = c.createStatement(); java.sql.ResultSet real = st.executeQuery(sql)) {
                    java.sql.ResultSet rs = recording(real, calls);
                    JdbcDecode.Reader reader = new JdbcDecode.Reader(cols,
                            new DatabaseConnector.Decoding(javaTime, false, false, false, true));
                    while (rs.next()) {
                        cells.add(reader.row(rs).get(0));
                    }
                }
                assertEquals(expected, cells, "javaTime " + javaTime);
                long direct = calls.stream().filter(m -> m.equals("getObject(LocalDateTime)")).count();
                if (javaTime) {
                    // row 1 is null; row 2 answers a Timestamp and is re-read; rows 3 to 5 are read as java.time at
                    // once, row 3's null told by wasNull alone
                    assertEquals(4, direct, calls.toString());
                    assertEquals(2, calls.stream().filter(m -> m.equals("getObject")).count(), calls.toString());
                    assertEquals(1, calls.stream().filter(m -> m.equals("wasNull")).count(), calls.toString());
                } else {
                    assertEquals(0, direct, calls.toString());
                }
            }
        }
    }

    /** {@code rs}, each getter it answers recorded in {@code calls} by name (and a typed getObject's class). */
    private static java.sql.ResultSet recording(java.sql.ResultSet rs, List<String> calls) {
        return (java.sql.ResultSet) java.lang.reflect.Proxy.newProxyInstance(
                JdbcDecodeH2Test.class.getClassLoader(), new Class<?>[] {java.sql.ResultSet.class},
                (proxy, method, args) -> {
                    if (method.getName().startsWith("get") || method.getName().equals("wasNull")) {
                        calls.add(method.getName().equals("getObject") && args.length == 2
                                ? "getObject(" + ((Class<?>) args[1]).getSimpleName() + ")" : method.getName());
                    }
                    try {
                        return method.invoke(rs, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
