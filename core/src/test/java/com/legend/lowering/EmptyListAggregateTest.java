// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
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
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            ExecutionResult r = Execution.execute(StorelessRuntime.with("", DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, c);
            if (r instanceof ExecutionResult.Scalar s) {
                return String.valueOf(s.value());
            }
            if (r instanceof ExecutionResult.Collection col) {
                return col.values().stream().map(String::valueOf).toList().toString();
            }
            throw new IllegalStateException("unexpected result for " + query + ": " + r);
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
        // was null; the product's DOUBLE spelling over an Integer list is DuckDB's, unchanged here
        assertEquals("1.0", scalar("|[1, 2, 3]->filter(x | $x > 5)->times()"));
    }

    @Test
    void nonEmptyListsAreUnchanged() throws Exception {
        assertEquals("5", scalar("|[1, 2, 3]->filter(x | $x > 1)->sum()"));
        assertEquals("6.0", scalar("|[1, 2, 3]->filter(x | $x > 1)->times()"));
        assertEquals("", scalar("|['a', 'b']->filter(x | $x == 'z')->plus()"));
    }

    @Test
    void anEmptySumPerRowIsZeroNotADroppedRow() throws Exception {
        // was a loud failure ("Cannot cast a collection of size 0 to multiplicity [2]"): the NULL cells were dropped
        assertEquals("[0, 0]", scalar("|[1, 2]->map(x | [10, 20]->filter(y | $y > $x * 100)->sum())"));
        assertEquals("[20, 0]", scalar("|[1, 2]->map(x | [10, 20]->filter(y | $y > $x * 12)->sum())"));
    }
}
