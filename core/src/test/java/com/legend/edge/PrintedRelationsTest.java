// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The door's lift of a program whose last step prints a relation ({@link PrintedRelations}; stage B's B2, B2b S2,
 * 2026-11-24): what is lifted, what is left to the run, and the program that remains.
 */
class PrintedRelationsTest {

    private static final String REL = "#TDS\n  a:Integer, b:String\n  1, x\n#";

    private static final String CSV = "->meta::relational::tests::csv::toCSV";

    private static com.legend.TypedQuery typed(String query) {
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(""), query);
    }

    private static PureRelationTextWriter.Form form(String query) {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed(query));
        assertNotNull(lifted, query);
        return lifted.form();
    }

    @Test
    void aFinalToStringIsLiftedAndTheRelationRemains() {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed("|" + REL + "->toString()"));
        assertNotNull(lifted);
        assertEquals(PureRelationTextWriter.Form.TO_STRING, lifted.form());
        assertEquals(List.of(), lifted.replaces());
        // the program that remains computes the relation: its result is the relation's, no longer a String
        assertEquals(typed("|" + REL).resultType(), lifted.relation().resultType());
    }

    @Test
    void literalReplacesAfterThePrintAreLiftedInTheProgramsOrder() {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed("|" + REL
                + "->toString()->replace('\\n', ',')->replace('x', 'y')"));
        assertNotNull(lifted);
        assertEquals(List.of(new PureRelationText.Replace("\n", ","), new PureRelationText.Replace("x", "y")),
                lifted.replaces());
    }

    @Test
    void whatTheDoorDoesNotLiftIsTheRunsAsBefore() {
        // a replace whose target, or whose replacement, is no literal
        assertNull(PrintedRelations.lift(typed("|let s = 'q'; " + REL + "->toString()->replace($s, ',');")));
        assertNull(PrintedRelations.lift(typed("|let s = 'q'; " + REL + "->toString()->replace(',', $s);")));
        // toString(rel, true): its header would not be legend-pure's (PureRelationText)
        assertNull(PrintedRelations.lift(typed("|" + REL + "->toString(true)")));
        // no print at the root
        assertNull(PrintedRelations.lift(typed("|" + REL + "->filter(r|$r.a == 1)")));
        // a print that is not the last step
        assertNull(PrintedRelations.lift(typed("|" + REL + "->toString()->length()")));
    }

    @Test
    void toStringFalseIsToString() {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed("|" + REL + "->toString(false)"));
        assertNotNull(lifted);
        assertEquals(PureRelationTextWriter.Form.TO_STRING, lifted.form());
    }

    @Test
    void theStatementsBeforeTheLastAreKept() {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed("|let n = 1; " + REL
                + "->filter(r|$r.a == $n)->toString();"));
        assertNotNull(lifted);
        com.legend.TypedQuery original = typed("|let n = 1; " + REL + "->filter(r|$r.a == $n)->toString();");
        assertEquals(2, lifted.relation().body().size());
        // the let as it was, and the printed relation, the print's own argument, in the print's place
        assertEquals(original.body().get(0), lifted.relation().body().get(0));
        assertEquals(((com.legend.compiler.spec.typed.TypedNativeCall) original.body().get(1)).args().get(0),
                lifted.relation().body().get(1));
    }

    @Test
    void theHelpersToCsvIsLiftedInEachOfItsForms() {
        assertEquals(PureRelationTextWriter.Form.TO_CSV, form("|" + REL + CSV + "()"));
        assertEquals(PureRelationTextWriter.Form.TO_CSV, form("|" + REL + CSV + "(false)"));
        assertEquals(PureRelationTextWriter.Form.TO_CSV_TDS_NULL, form("|" + REL + CSV + "(true)"));
        // the default formats are the two-argument form's own (helperFunctions.pure:203-206: SimpleDateTimeFormat()
        // and ISO8601DateFormat(), dateExtension.pure:384-392, which return these two texts), as the lowering reads them
        assertEquals(PureRelationTextWriter.Form.TO_CSV_TDS_NULL,
                form("|" + REL + CSV + "('%t{yyyy-MM-dd HH:mm:ss}', '%t{yyyy-MM-dd}', true)"));
        assertEquals(PureRelationTextWriter.Form.TO_CSV,
                form("|" + REL + CSV + "('%t{yyyy-MM-dd HH:mm:ss}', '%t{yyyy-MM-dd}', false)"));
        // any other formats are the run's
        assertNull(PrintedRelations.lift(typed("|" + REL + CSV + "('yyyy', 'yyyy-MM-dd', false)")));
    }

    @Test
    void aTdsCsvPropertyOverARelationIsLiftedAndOverACastIsTheRuns() {
        assertEquals(PureRelationTextWriter.Form.TDS_CSV, form("|" + REL + ".csv"));
        // the read over a cast to a TDS class is the resolver's (Anchors.tdsCsvRead): not lifted
        assertNull(PrintedRelations.lift(typed("|" + REL + "->cast(@meta::pure::metamodel::relation::TDS<Any>).csv")));
    }

    @Test
    void aRelationWhoseColumnsTheDatabaseNamesIsTheRuns() {
        // a dynamic pivot whose columns nothing names: the lowering refuses its text by name
        // (DynamicPivotTextRefusalTest), and the door leaves it to the run
        String pivot = "|#TDS\nYR,CITY,TREES\n2000,NYC,1\n2000,SF,2\n#->pivot(~CITY, ~total : x|$x.TREES : y|$y->sum())";
        assertNull(PrintedRelations.lift(typed(pivot + "->toString()")));
        assertNull(PrintedRelations.lift(typed(pivot + CSV + "()")));
    }

    @Test
    void theLiftedQueryKeepsTheQuerysOptions() {
        com.legend.TypedQuery query = typed("|" + REL + "->toString()")
                .withOptions(java.util.Set.of(com.legend.platform.PlanOption.DETERMINISTIC_AS_OF));
        PrintedRelations.Lifted lifted = PrintedRelations.lift(query);
        assertNotNull(lifted);
        assertEquals(query.options(), lifted.relation().options());
    }
}
