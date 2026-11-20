// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A GUARDED RAISE INSIDE A LAMBDA, ON THE EMBEDDED CLICKHOUSE (2026-11-20, the warm lane's finding): ClickHouse evaluates
 * every branch of a CASE inside a lambda for every element, so a substring's range guard in {@code map}'s lambda raised
 * {@code SUBSTRING_OUT_OF_RANGE} over valid text. On the product road: valid text answers, and a text out of range still
 * raises the product's kind. The SQL's form is {@code ClickHouseLambdaRaiseTest}'s. Its own target
 * ({@code //core:clickhouse_lambda_raise_test}): it needs the embedded ClickHouse in its runfiles.
 */
class ClickHouseLambdaRaiseServerTest {

    @Test
    void aGuardedRaiseInALambdaRaisesWhereItsGuardHoldsAlone() throws Exception {
        TestBackend backend = TestBackends.of(DatabaseType.ClickHouse);
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            Lean.Result ok = Lean.run(model,
                    "|['Doe', 'Smith', 'Branche']->map(x|$x->substring(1, 2))->joinStrings(',')",
                    StorelessRuntime.RUNTIME, c);
            assertEquals("o,m,r", ok.rows().get(0).get(0), "every element in range: no raise");
            Exception e = assertThrows(Exception.class, () -> Lean.run(model,
                    "|['Doe', 'S', 'Branche']->map(x|$x->substring(1, 2))->joinStrings(',')",
                    StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e.getMessage()).contains(
                    com.legend.error.ErrorKind.SUBSTRING_OUT_OF_RANGE.sentence()),
                    "an element out of range still raises the product's kind: " + e.getMessage());
        }
    }

    /** The range raise this dialect writes for date arithmetic (ClickHouse clamps a date past 2299): dates in range,
     *  adjusted in a map's lambda, answer; one adjusted past 2299 still raises the product's kind. */
    @Test
    void dateArithmeticInALambdaRaisesPastTheYearsAlone() throws Exception {
        TestBackend backend = TestBackends.of(DatabaseType.ClickHouse);
        String model = backend.withStorelessRuntime("");
        String adjust = "->map(d|$d->meta::pure::functions::date::adjust(1, meta::pure::functions::date::DurationUnit.DAYS)"
                + "->meta::pure::functions::date::dayOfMonth())->sum()";
        try (Connection c = backend.open()) {
            Lean.Result ok = Lean.run(model, "|[%2024-01-01, %2024-02-01]" + adjust, StorelessRuntime.RUNTIME, c);
            assertEquals(4L, ((Number) ok.rows().get(0).get(0)).longValue(), "both in range: 2 + 2");
            Exception e = assertThrows(Exception.class, () -> Lean.run(model, "|[%2024-01-01, %2299-12-31]" + adjust,
                    StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e.getMessage()).contains(com.legend.error.ErrorKind.VALUE_NOT_HELD.sentence()),
                    "a date adjusted past 2299 still raises the product's kind: " + e.getMessage());
        }
    }
}
