// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE MIXED LIST'S FLOOR (E-Java's find, 2026-11-16; the lean program's E-v2): two wrong answers on the variant and
 * number carriers, each now refused or raised by name, never answered wrong, on every in-process lane.
 * <ol>
 *   <li>A list of any kinds sorted on the variant carrier sorted by the variant's order, not Pure's across kinds
 *       ({@code range(...)->concatenate(['a'])->sort()} put {@code 'a'} first): refused at planning by name.</li>
 *   <li>An Integer list concatenated with a Float list is a Number list promoted to Float; an Integer past 2^53 became
 *       the nearest Float (9007199254740993 read 9007199254740992.0): where the carrier is a DOUBLE it raises
 *       {@code VALUE_NOT_HELD} at run time, where only the values tell; where the carrier holds it exactly (DuckDB's
 *       DECIMAL, SQLite's 64-bit integer) it answers exactly. Never the nearest Float.</li>
 * </ol>
 */
class MixedListFloorTest extends AbstractDatabaseTest {

    @BeforeEach
    void setUp() throws SQLException {
        connection = backend().open();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("a sort of a list of mixed kinds on the variant carrier is refused by name, never a wrong order")
    void aSortAcrossKindsIsRefused() {
        Exception e = assertThrows(Exception.class,
                () -> generateSql("|range(3)->reverse()->concatenate(['a'])->sort()"));
        assertTrue(String.valueOf(e.getMessage()).contains("mixed kinds"), String.valueOf(e.getMessage()));
    }

    @Test
    @DisplayName("an Integer past 2^53 in a list with a Float is exact or raises, never the nearest Float")
    void anIntegerPastTwoToTheFiftyThreeIsExactOrRaises() throws SQLException {
        Object max;
        try {
            var rows = executeRelation(
                    "|range(9007199254740990, 9007199254740995)->concatenate([2.5])->max()").rows();
            max = rows.get(0).get(0);
        } catch (com.legend.sql.DialectCapability unsupported) {
            // a lane with no list encoding refuses by name: never an answer
            assertTrue(String.valueOf(unsupported.getMessage()).contains("list encoding"), unsupported.getMessage());
            return;
        } catch (Exception raised) {
            assertTrue(String.valueOf(raised.getMessage()).contains("2^53"), String.valueOf(raised.getMessage()));
            return;
        }
        // exact where the carrier holds it (DuckDB's DECIMAL, SQLite's 64-bit integer): never 9007199254740992
        assertEquals(0, new java.math.BigDecimal("9007199254740994").compareTo(new java.math.BigDecimal(
                String.valueOf(max))), String.valueOf(max));
    }

    @Test
    @DisplayName("Integers a Float holds exactly still answer in a list with a Float")
    void smallIntegersStillAnswer() throws SQLException {
        try {
            var rows = executeRelation("|range(3)->concatenate([2.5])->max()").rows();
            assertEquals(2.5, ((Number) rows.get(0).get(0)).doubleValue(), 0.0);
        } catch (com.legend.sql.DialectCapability unsupported) {
            // a lane with no list encoding refuses by name
            assertTrue(String.valueOf(unsupported.getMessage()).contains("list encoding"), unsupported.getMessage());
        }
    }
}
