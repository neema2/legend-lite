// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.testing.Lean;

import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import com.legend.test.StorelessRuntime;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pure's {@code plus([])} and {@code sum([])} are 0 and {@code times([])} is 1 ({@code plus.pure:20-23}, the
 * interpreter's {@code Plus.java} case 0); DuckDB's list aggregates over an empty list are NULL, so the list forms
 * returned null (rebuild W0.6 push 11, homework 4 H). The group and window forms are not touched: over an all-NULL
 * group PCT and the engine expect NULL. Each case's old value is in its comment.
 */
class EmptyListAggregateTest {

    private static String scalar(String query) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Lean.Result r = Lean.run(StorelessRuntime.with("", Databases.PLATFORM), query, StorelessRuntime.RUNTIME, c);
            Object v = r.value();
            return v instanceof List<?> l ? l.stream().map(String::valueOf).toList().toString() : String.valueOf(v);
        }
    }

    @Test
    void sumAndPlusOfAnEmptyListAreZero() throws Exception {
        assertEquals("0", scalar("|[1, 2, 3]->filter(x | $x > 5)->sum()"));    // was null
        assertEquals("0", scalar("|[1, 2, 3]->filter(x | $x > 5)->plus()"));   // was null
        assertEquals("0.0", scalar("|[1.5, 2.5]->filter(x | $x > 5.0)->sum()"));   // was null
        assertEquals("0.0", scalar("|[1.5d, 2.5d]->filter(x | $x > 5.0d)->sum()"));   // was null
    }

    @Test
    void timesOfAnEmptyListIsOne() throws Exception {
        // was null, then 1.0 (DuckDB's DOUBLE product); an Integer list's product is the Integer 1 (2026-10-27, the
        // result boundary program's phase 5: the plan's typed decode refused a Float under the Integer root)
        assertEquals("1", scalar("|[1, 2, 3]->filter(x | $x > 5)->times()"));
    }

    @Test
    void nonEmptyListsAreUnchanged() throws Exception {
        assertEquals("5", scalar("|[1, 2, 3]->filter(x | $x > 1)->sum()"));
        assertEquals("6", scalar("|[1, 2, 3]->filter(x | $x > 1)->times()"));   // was 6.0: an Integer's product
        assertEquals("", scalar("|['a', 'b']->filter(x | $x == 'z')->plus()"));
    }

    @Test
    void anEmptySumPerRowIsZeroNotADroppedRow() throws Exception {
        // was a loud failure ("Cannot cast a collection of size 0 to multiplicity [2]"): the NULL cells were dropped
        assertEquals("[0, 0]", scalar("|[1, 2]->map(x | [10, 20]->filter(y | $y > $x * 100)->sum())"));
        assertEquals("[20, 0]", scalar("|[1, 2]->map(x | [10, 20]->filter(y | $y > $x * 12)->sum())"));
    }
}
