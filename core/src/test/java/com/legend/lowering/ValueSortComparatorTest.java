// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.testing.Lean;

import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.StorelessRuntime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Value-collection sort with a COMPARATOR/KEY function executes in SQL
 * (list-sort machinery) — a capability the reference DuckDB adapter
 * excludes outright ("No SQL translation exists for sort with
 * comparator"). These pins hold the machinery green; the PCT key-sort
 * tests remain excluded only for the SUBSTRING 1-based relational
 * divergence their keys ride on, never for the sort itself.
 */
class ValueSortComparatorTest {

    private static final String MODEL = "Class test::Dummy { x: String[1]; }";

    private static List<Object> col(String query) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            return Lean.run(StorelessRuntime.with(MODEL, Databases.PLATFORM), query, StorelessRuntime.RUNTIME, c).rows().stream()
                    .map(r -> r.get(0)).toList();
        }
    }

    @Test
    @DisplayName("descending comparator sorts DESC in SQL")
    void descendingComparator() throws Exception {
        assertEquals(List.of("Smith", "Doe", "Branche"),
                col("|['Smith', 'Branche', 'Doe']->meta::pure::functions"
                        + "::collection::sort({x: String[1], y: String[1]|"
                        + "$y->meta::pure::functions::lang::compare($x)})"));
    }

    @Test
    @DisplayName("key-function sort orders by the key (Pure's 0-based substring keys)")
    void keyFunctionSort() throws Exception {
        // keys are Pure's substrings (S130): o/m/r -> m,o,r, PCT's sort::testSimpleSortWithKey answer (sort.pure:108).
        // Re-pinned by A3 of the compatibility program, piece 2, 2026-11-19 (the user's word: "approved: push both A3
        // branches and make piece 2's moves"); it held legend-engine's 1-based keys, Do/Sm/Br -> Br,Do,Sm
        assertEquals(List.of("Smith", "Doe", "Branche"),
                col("|['Doe', 'Smith', 'Branche']->meta::pure::functions"
                        + "::collection::sort(s: String[1]|$s->meta::pure"
                        + "::functions::string::substring(1, 2), "
                        + "{x: String[1], y: String[1]|$x->meta::pure"
                        + "::functions::lang::compare($y)})"));
    }
}
