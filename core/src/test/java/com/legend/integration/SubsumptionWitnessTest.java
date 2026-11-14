// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE SUBSUMPTION RECEIPTS (charter §4bZ-V B2): the two rows of
 * {@link com.legend.sql.SqlTyping#subsumes} are lossless
 * subtype-in-supertype-slot relations, and these witnesses PROVE the
 * losslessness at the value level — the decode under the supertype
 * label is identical to the subtype's own decode (the Executor fetch
 * switch is driver-object-kind-keyed, so a label never coerces a
 * value). Each witness reproduces the census's own dominant traffic
 * shape (testAdjustByYears / testDecimalAbs).
 */
public class SubsumptionWitnessTest extends AbstractDatabaseTest {

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
        void testTimestampSlotKeepsStrictDateValue() throws SQLException {
                // adjust() on a StrictDate declares abstract Date (label
                // TIMESTAMP — where abstract Date erases) while the wire
                // delivers CAST(ADD_INTERVAL(…) AS DATE). The round trip
                // must yield the DAY-precision pure value — never a
                // midnight timestamp the label alone would suggest.
                var result = Lean.run(getCompletePureModelWithRuntime(), "|%2024-03-05->adjust(1, meta::pure::functions::date::DurationUnit.YEARS)", "test::TestRuntime", connection);
                assertEquals(com.legend.values.PureDateLiteral.parse("2025-03-05"),
                                result.rows().get(0).get(0));
        }

        @Test
        void testWideDecimalSlotKeepsNarrowValue() throws SQLException {
                // abs() on a decimal literal declares the pure-Decimal
                // erasure label (38,1) while the wire delivers the
                // literal's own (2,1). Exact BigDecimal equality (value
                // AND scale) is the losslessness proof.
                var result = Lean.run(getCompletePureModelWithRuntime(), "|abs(-1.5D)", "test::TestRuntime", connection);
                assertEquals(new BigDecimal("1.5"),
                                result.rows().get(0).get(0));
        }
}
