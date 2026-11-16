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
 * A FLOAT CARRIED IN A LIST READS BACK EXACTLY (E-v3, 2026-11-16; the total-wall plan's carrier caution, §5 item 2):
 * a computed double -- not a short literal, which any text holds -- put in a list and read back out is the same
 * double, on every in-process lane. A carrier that holds the value as text holds it only as exactly as the database's
 * text of a double reads back (SQLite's JSON writes fifteen significant digits, measured on 3.53.1).
 */
class FloatCarrierExactTest extends AbstractDatabaseTest {

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

    /** The answer of {@code query} is {@code exact}; a lane with no list encoding refuses by name (H2). Never a
     *  rounded double. */
    private void readsBack(String query, double exact) {
        Object v;
        try {
            v = executeRelation(query).rows().get(0).get(0);
        } catch (com.legend.sql.DialectCapability unsupported) {
            assertTrue(String.valueOf(unsupported.getMessage()).contains("list encoding"), unsupported.getMessage());
            return;
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
        assertEquals(exact, ((Number) v).doubleValue(), query);
    }

    @Test
    @DisplayName("a computed Float read back out of a list is the same double")
    void aComputedFloatInAListReadsBackExactly() {
        readsBack("|[sqrt(2.0), 0.5]->max()", Math.sqrt(2.0));
    }

    @Test
    @DisplayName("a computed Float's list sorted and read is the same double")
    void aSortedComputedFloatReadsBackExactly() {
        readsBack("|[sqrt(3.0), sqrt(2.0)]->sort()->at(0)", Math.sqrt(2.0));
    }
}
