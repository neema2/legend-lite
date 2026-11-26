// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Databases;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * ONE VALUE WITHIN ONE TYPE (the equality rule, AGENTS.md: "a value is what it denotes, not how it was written"; the
 * edition program's step 6, slice 6.1, 2026-11-26): two date-times at one instant are equal whatever precision each
 * was written at (the user's ruling S399), and two Decimals of one number whatever their written scale (S139) -- in
 * the product's own {@code ==}, {@code in} and {@code contains}. The DATABASE answers each: until this slice the
 * compiler's fold answered a comparison of two spelled date-times itself, by their written text, and said false
 * where the database says true. By the fold rule the compiler compares constant Strings, Booleans and enumeration
 * values alone ({@code LiteralUnroll.comparedHere}); numbers, Decimals and dates go to the database.
 */
class OneValueWithinOneTypeTest {

    private static List<String> values(String query) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Lean.Result r = Lean.run(StorelessRuntime.with("", Databases.PLATFORM), query, StorelessRuntime.RUNTIME, c);
            if (!(r.value() instanceof List<?> col)) {
                throw new IllegalStateException("expected a collection frame for " + query + ", got " + r);
            }
            return col.stream().map(String::valueOf).toList();
        }
    }

    @Test
    @DisplayName("two date-time literals: equal by their instant, at any written precision")
    void dateTimeLiteralsCompareByInstant() throws Exception {
        assertEquals(List.of("true", "true", "true", "false", "false"), values("|["
                + "%2020-01-01T10:00:00 == %2020-01-01T10:00:00.000, "
                + "%2020-01-01T10:00:00.5 == %2020-01-01T10:00:00.500, "
                + "%2020-01-01T10:00:00.000 == %2020-01-01T10:00:00.000, "
                + "%2020-01-01T10:00:00.5 == %2020-01-01T10:00:00.6, "
                + "%2020-01-01T10:00:00 == %2020-01-01T10:00:01]"));
    }

    @Test
    @DisplayName("a date-time literal among date-time literals: a member by its instant")
    void dateTimeLiteralsAreMembersByInstant() throws Exception {
        assertEquals(List.of("true", "false"), values("|["
                + "%2020-01-01T10:00:00->in([%2020-01-01T09:00:00, %2020-01-01T10:00:00.000]), "
                + "%2020-01-01T10:00:00.5->in([%2020-01-01T10:00:00, %2020-01-01T10:00:00.6])]"));
    }

    @Test
    @DisplayName("two Decimal literals: equal by their number, at any written scale")
    void decimalLiteralsCompareByNumber() throws Exception {
        assertEquals(List.of("true", "true", "false"), values("|["
                + "1.0D == 1.00D, 0.3D == 0.30D, 1.5D == 1.50001D]"));
    }
}
