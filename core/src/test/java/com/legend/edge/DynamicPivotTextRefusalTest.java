// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A DYNAMIC PIVOT'S TEXT OVER COLUMNS NOTHING NAMES is refused by name where the edge would compose it (J40, the
 * orchestrator's ruling (8), 2026-11-18): its columns exist only once the database is asked, and planning asks the
 * database nothing. Until then the edge built a deferred node (SqlExpr.DeferredTdsString) the body planner refused after
 * lowering, in the same words; the node is deleted, the words pinned here, for toString and toCSV.
 */
class DynamicPivotTextRefusalTest {

    private static final String PIVOT = "|#TDS\nYR,CITY,TREES\n2000,NYC,1\n2000,SF,2\n#"
            + "->pivot(~CITY, ~total : x|$x.TREES : y|$y->sum())";

    @Test
    void aDynamicPivotsTextIsRefusedByNameAtLowering() throws Exception {
        TestBackend backend = TestBackends.platform();
        String model = backend.withStorelessRuntime("");
        for (String text : List.of("->meta::pure::functions::relation::toString()",
                "->meta::relational::tests::csv::toCSV()")) {
            Exception refused = assertThrows(Exception.class, () -> com.legend.testing.Lean.runInStyle(model,
                    PIVOT + text, com.legend.testbackend.StorelessRuntime.RUNTIME, backend, PureText.UPSTREAM), text);
            assertTrue(chain(refused).contains("a dynamic pivot's text (toString/toCSV) over columns nothing names:"
                    + " its columns exist only once the database is asked, planning asks the database nothing, and the"
                    + " pivot's keyed form writes no text"), text + ": " + chain(refused));
        }
    }

    private static String chain(Throwable t) {
        StringBuilder out = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            out.append(c.getClass().getSimpleName()).append(": ").append(c.getMessage()).append(" | ");
        }
        return out.toString();
    }
}
