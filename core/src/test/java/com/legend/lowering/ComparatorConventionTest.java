// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The comparator-convention roster's REFEREE-SILENT corner, pinned
 * (M4 post-landing audit 2026-08-25): {@code contains(col, value,
 * comparator)} over a hetero LITERAL-carried list. No PCT or corpus
 * test exercises this shape, which is exactly how the M4 re-land
 * shipped it broken — the comparator's toString body froze on the
 * variant path while the collection rode spellings (Malformed JSON on
 * {@code "'1'"}). The cure is contains' membership in
 * {@code Scalars.COMPARATOR_NATIVES}: both comparator params stamp as
 * the list's element at body lowering, so dispatch inside the body
 * reads the carrier from the tree.
 */
class ComparatorConventionTest {

    private static final String MODEL = "Class test::Dummy { x: String[1]; }";
    private static final String FQ = "meta::pure::functions::";

    private static Object one(String q) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            return ((ExecutionResult.Scalar) Compiler.execute(StorelessRuntime.with(MODEL, DatabaseType.DuckDB), q, StorelessRuntime.RUNTIME, c))
                    .value();
        }
    }

    @Test
    @DisplayName("contains w/ toString comparator over a hetero list — hit")
    void containsComparatorHeteroHit() throws Exception {
        assertEquals(Boolean.TRUE, one(
                "|[1, 2, '1']->" + FQ + "collection::contains('2', "
                + "{x,y|$x->" + FQ + "string::toString() == $y->"
                + FQ + "string::toString()})"));
    }

    @Test
    @DisplayName("contains w/ toString comparator over a hetero list — miss")
    void containsComparatorHeteroMiss() throws Exception {
        assertEquals(Boolean.FALSE, one(
                "|[1, 2, '1']->" + FQ + "collection::contains('3', "
                + "{x,y|$x->" + FQ + "string::toString() == $y->"
                + FQ + "string::toString()})"));
    }

    @Test
    @DisplayName("contains w/ kind-honest eq comparator: '1' is not 1")
    void containsComparatorKindHonest() throws Exception {
        // the carrier's whole point: cross-kind is FALSE even when the
        // texts collide — the comparator sees spellings-decoded values
        assertEquals(Boolean.FALSE, one(
                "|[1, 2, 'x']->" + FQ + "collection::contains('1', "
                + "{x,y|$x->" + FQ + "boolean::eq($y)})"));
    }
}
