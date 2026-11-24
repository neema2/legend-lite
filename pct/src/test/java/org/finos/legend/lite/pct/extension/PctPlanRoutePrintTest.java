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
        PctPlanRoute.Answer answer = PctPlanRoute.execute("",
                "|#TDS\n  i:Integer, s:String\n  1, a\n  2, \n#->sort(~i->ascending())"
                        + "->meta::relational::tests::csv::toCSV(true)->replace('\\n', ';')");
        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, answer.shape());
        assertEquals(List.of(List.of("i,s;1,a;2,TDSNull;")), answer.rows().rows());
    }
}
