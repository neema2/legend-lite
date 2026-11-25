// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import com.legend.executionplan.ExecutionPlan;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Channel A's lifted print, end to end (stage B's B2b S2, 2026-11-24): at legend-pure's door (the default audience) a
 * case whose last step prints a relation runs the relation and answers legend-pure's text, printed in Java from its typed
 * rows ({@code edge.PrintedRelations}, {@code edge.PureRelationTextWriter}), as one String value. No PCT case ends in a
 * print today (measured 2026-11-24: no channel A answer on any edge-net lane is a printed table), so this test is the
 * branch's one run.
 */
class PctPlanRoutePrintTest {

    @Test
    void aFinalToStringAnswersLegendPuresTextOfTheRows() throws Exception {
        PctPlanRoute.Answer answer = PctPlanRoute.execute("",
                "|#TDS\n  i:Integer, d:Date\n  1, 2014-01-02\n  2, 2020-02-29\n#->sort(~i->ascending())->toString()");
        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, answer.shape());
        // each day as legend-pure prints it: the edge's SQL text wrote a midnight date-time
        // (PureRelationTextParityTest.whereTheEdgesSqlTextIsWrongTheJavaPrinterWritesLegendPures)
        assertEquals(List.of(List.of("#TDS\n   i,d\n   1,2014-01-02\n   2,2020-02-29\n#")), answer.rows().rows());
    }

    @Test
    void theHelpersToCsvAndItsLiteralReplacesAreApplied() throws Exception {
        // a Float of 1e23: legend-pure prints 99999999999999990000000.0, the edge's SQL text on DuckDB
        // 99999999999999991611392.0 (PureRelationTextParityTest), so the text proves the lifted branch ran
        PctPlanRoute.Answer answer = PctPlanRoute.execute("",
                "|#TDS\n  i:Integer, f:Float\n  1, 1.0E23\n  2, \n#->sort(~i->ascending())"
                        + "->meta::relational::tests::csv::toCSV(true)->replace('\\n', ';')");
        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, answer.shape());
        assertEquals(List.of(List.of("i,f;1,99999999999999990000000.0;2,TDSNull;")), answer.rows().rows());
    }

    /** A MIXED SELECTION IN A COLUMN (the review of 6fc717f9f, 2026-11-24, asked whether the door lifts a print the
     *  edge's SQL text answered): a relation whose column holds a mixed max is refused by name on both roads today --
     *  the lifted print (the relation run alone) and a print the door does not lift (the edge's SQL text, here a
     *  print that is not the last step) -- since a tagged value reaches the renderer only at a value's root. The
     *  printer's own refusal of a tagged column (PureRelationText.requirePrintable) waits for the day a relation's
     *  column can be one. */
    @Test
    void aMixedSelectionInAColumnIsRefusedOnBothRoads() {
        String relation = "|#TDS\n  i:Integer\n  1\n#->extend(~m:x|[1, 2.5]->max())";
        // the lifted print (the relation run alone) and a print the door does not lift (lite's own text, computed by the
        // database since stage B's B2b S5a, 2026-11-24), each refused by name in its own words
        java.util.Map<String, String> words = java.util.Map.of("->toString()", "a tagged value reached the renderer",
                "->toString()->length()", "a relation's toString over a mixed column: its cells' kinds differ by row");
        for (var tail : words.entrySet()) {
            Exception refused = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                    () -> PctPlanRoute.execute("", relation + tail.getKey()), tail.getKey());
            String chain = "";
            for (Throwable t = refused; t != null; t = t.getCause()) {
                chain += t.getMessage() + " | ";
            }
            org.junit.jupiter.api.Assertions.assertTrue(chain.contains(tail.getValue()), tail.getKey() + ": " + chain);
        }
    }
}
