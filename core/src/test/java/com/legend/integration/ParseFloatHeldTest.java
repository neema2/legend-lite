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
}
