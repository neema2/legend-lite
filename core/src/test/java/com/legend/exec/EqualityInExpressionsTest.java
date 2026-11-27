// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * {@code ==} INSIDE AN EXPRESSION ANSWERS AS A COMPARISON OF TWO WHOLE SIDES DOES (the edition program's step 6, the
 * one equality, 2026-11-27): the cases where the two forms once differed, each held here as it is brought to the one
 * meaning. On the platform's own database, whose lists are values an expression can compare.
 */
class EqualityInExpressionsTest {

    private static List<List<Object>> rows(String query) throws Exception {
        var backend = TestBackends.platform();
        try (Connection connection = backend.open()) {
            Lean.Result r = Lean.established(StorelessRuntime.with("", backend.type()), query, StorelessRuntime.RUNTIME,
                    connection);
            return r.rows();
        }
    }

    @Test
    void aComputedListOfNoElementEqualsTheEmptyList() throws Exception {
        // for each x, the elements of [1, 2] above it: none for 2 and for 6
        assertEquals(List.of(List.of(2L), List.of(6L)),
                rows("|[1, 2, 6]->filter(x | [1, 2]->filter(y | $y > $x) == [])"));
        assertEquals(List.of(List.of(2L), List.of(6L)),
                rows("|[1, 2, 6]->filter(x | [] == [1, 2]->filter(y | $y > $x))"));
        assertEquals(List.of(List.of(1L)),
                rows("|[1, 2, 6]->filter(x | !([1, 2]->filter(y | $y > $x) == []))"));
    }
}
