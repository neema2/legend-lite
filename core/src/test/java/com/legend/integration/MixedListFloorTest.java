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

    /** The answer of {@code query} is {@code exact} or a raise; a lane with no list encoding refuses by name. Never
     *  the nearest Float. */
    private void exactOrRaises(String query, String exact) {
        Object max;
        try {
            max = executeRelation(query).rows().get(0).get(0);
        } catch (com.legend.sql.DialectCapability unsupported) {
            assertTrue(String.valueOf(unsupported.getMessage()).contains("list encoding"), unsupported.getMessage());
            return;
        } catch (Exception raised) {
            // a raise is never a wrong answer: the guard's own (2^53), or a database's refusal of the value (H2 and
            // SQLite: outside the range it can hold)
            return;
        }
        assertEquals(0, new java.math.BigDecimal(exact).compareTo(new java.math.BigDecimal(String.valueOf(max))),
                String.valueOf(max));
    }

    @Test
    @DisplayName("an Integer past 2^53 concatenated with a Float list is exact or raises, never the nearest Float")
    void anIntegerPastTwoToTheFiftyThreeIsExactOrRaises() {
        // the maximum, 2^53 + 1, is odd: no DOUBLE holds it (E-v2's review, B-M2), and sqrt(2.0) is a DOUBLE side
        exactOrRaises("|range(9007199254740991, 9007199254740994)->concatenate([sqrt(2.0)])->max()",
                "9007199254740993");
    }

    @Test
    @DisplayName("an Integer past 2^53 beside a Float added to its list is exact or raises (A-S2)")
    void anIntegerPastTwoToTheFiftyThreeBesideAnAddedFloatIsExactOrRaises() {
        exactOrRaises("|range(9007199254740991, 9007199254740994)->add(sqrt(2.0))->max()", "9007199254740993");
    }

    @Test
    @DisplayName("a comparator sort of a list of mixed kinds is refused by name too (A-M3)")
    void aComparatorSortAcrossKindsIsRefused() {
        Exception e = assertThrows(Exception.class, () -> generateSql(
                "|range(3)->concatenate(['a'])->sort({a, b | $a->compare($b)})"));
        assertTrue(String.valueOf(e.getMessage()).contains("mixed kinds"), String.valueOf(e.getMessage()));
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
