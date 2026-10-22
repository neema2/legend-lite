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
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:jdbcdecode" + H2Connector.SETTINGS)) {
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

    /** A database whose driver re-reads no timestamp as java.time keeps the driver's Timestamp, the same date-time
     *  for an AD year (2026-10-21: Snowflake's driver refuses the re-read, and the typed seam re-read every timestamp
     *  regardless; the legacy executor's rule, {@code SqlDialect.refetchesTimestampAsJavaTime}). */
    @Test
    void aTimestampTheDriverDoesNotReReadIsTheDriversOwn() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:jdbcdecode_keep" + H2Connector.SETTINGS);
                java.sql.Statement st = c.createStatement();
                java.sql.ResultSet rs = st.executeQuery("SELECT TIMESTAMP '2024-01-02 10:30:00.123456' AS TS")) {
            rs.next();
            assertTrue(rs.getObject(1) instanceof java.sql.Timestamp, "H2 hands a TIMESTAMP back as a Timestamp");
            List<ExecutionPlan.Column> cols = List.of(JdbcDecodeCases.col("TS", "DateTime", "TIMESTAMP"));
            assertEquals(List.of(PureDateLiteral.parse("2024-01-02T10:30:00.123456000")),
                    JdbcDecode.row(rs, cols, false));
            assertEquals(JdbcDecode.row(rs, cols, true), JdbcDecode.row(rs, cols, false));
        }
    }
}
