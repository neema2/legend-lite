// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A FLOAT PARSED FROM TEXT IS THE VALUE ITS TEXT NAMES, OR REFUSED BY NAME (Phase 8, 2026-11-16; D's probe: on SQLite
 * parseFloat('NaN') answered 0, SQLite's CAST reading 'NaN' and 'Infinity' as 0): NaN where the database holds it,
 * VALUE_NOT_HELD where it cannot (register S138); an infinity as the database's own; never a silent 0.
 */
class ParseFloatHeldTest extends AbstractDatabaseTest {

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

    private Object one(String query) throws SQLException {
        return executeRelation(query).rows().get(0).get(0);
    }

    @Test
    @DisplayName("parseFloat of NaN is NaN, or refused by name where the database cannot hold it; never 0")
    void nanIsNaNOrNotHeld() {
        Object v;
        try {
            v = one("|'NaN'->parseFloat()");
        } catch (Exception e) {
            assertTrue(String.valueOf(e.getMessage()).contains("cannot hold"), String.valueOf(e.getMessage()));
            return;
        }
        assertTrue(Double.isNaN(((Number) v).doubleValue()), String.valueOf(v));
    }

    @Test
    @DisplayName("parseFloat of an infinity is that infinity; of a number, that number")
    void infinitiesAndNumbers() throws SQLException {
        assertEquals(Double.POSITIVE_INFINITY, ((Number) one("|'Infinity'->parseFloat()")).doubleValue());
        assertEquals(Double.NEGATIVE_INFINITY, ((Number) one("|'-Infinity'->parseFloat()")).doubleValue());
        assertEquals(1.5, ((Number) one("|'1.5'->parseFloat()")).doubleValue());
    }

    @Test
    @DisplayName("parseFloat of text that is no number raises, on every lane: never SQLite's silent 0")
    void textThatIsNoNumberRaises() {
        for (String text : java.util.List.of("abc", "", "0x10", "1.2.3", "1e", "-", "+", ".", "e5", "0-9e4", "+-1", "1_000", "nan", "inf")) {
            try {
                Object v = one("|'" + text + "'->parseFloat()");
                throw new AssertionError("'" + text + "' answered " + v);
            } catch (AssertionError e) {
                throw e;
            } catch (Exception raised) {
                // a raise is the answer (legend-pure's and DuckDB's)
            }
        }
    }

    @Test
    @DisplayName("parseFloat of a number is that number, or refused by name where SQLite's test cannot read it")
    void numbersAreExactOrRefusedByName() throws SQLException {
        java.util.Map<String, Double> numbers = new java.util.LinkedHashMap<>();
        numbers.put("1", 1.0);
        numbers.put("-0.5", -0.5);
        numbers.put("1e5", 1e5);
        numbers.put("1E+5", 1e5);
        numbers.put("+1", 1.0);
        numbers.put(".5", 0.5);
        numbers.put("1.", 1.0);
        // legend-pure's own PCT inputs (parseFloat.pure: testParseFloat, testParseZero) and the zeros' forms
        numbers.put("+0000003.1415900000", 3.14159);
        numbers.put("-0000003.1415900000", -3.14159);
        numbers.put("-000.000", -0.0);
        numbers.put("01", 1.0);
        numbers.put("-01", -1.0);
        numbers.put("00.5", 0.5);
        numbers.put("1.e5", 1e5);
        // one rule on every database (the orchestrator, 2026-11-16): whitespace lenient, one trailing f accepted
        numbers.put(" 1 ", 1.0);
        numbers.put("1f", 1.0);
        numbers.put("1.5F", 1.5);
        for (var n : numbers.entrySet()) {
            Object v;
            try {
                v = one("|'" + n.getKey() + "'->parseFloat()");
            } catch (Exception refused) {
                throw new AssertionError(n.getKey() + " refused: " + refused.getMessage(), refused);
            }
            // Pure's equality: 0.0 == -0.0 (parseFloat.pure's testParseZero)
            assertTrue(n.getValue() == ((Number) v).doubleValue(), n.getKey() + " answered " + v);
        }
    }

    @Test
    @DisplayName("the parse table: each input's answer on this lane, printed for the lane rounds (the parse family's MUST)")
    void printsTheParseTable() {
        String lane = backend().name();
        for (String fn : java.util.List.of("parseFloat", "parseInteger")) {
            for (String text : java.util.List.of("1", "-0.5", "1e5", "+1", ".5", "1.", " 1 ", "abc", "", "0x10",
                    "1.2.3", "Infinity", "NaN", "1f", "01", "+-1", "1e")) {
                String answer;
                try {
                    answer = String.valueOf(one("|'" + text + "'->" + fn + "()"));
                } catch (Exception raised) {
                    String m = String.valueOf(raised.getMessage());
                    answer = "RAISES " + (m.length() > 80 ? m.substring(0, 80) : m).replace('\n', ' ');
                }
                System.out.println("[parse-table] " + lane + " " + fn + " '" + text + "' -> " + answer);
            }
        }
    }
}
