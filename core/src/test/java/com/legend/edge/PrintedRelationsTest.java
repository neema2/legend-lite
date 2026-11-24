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

    private static com.legend.TypedQuery typed(String query) {
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(""), query);
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
        // a replace whose argument is no literal
        assertNull(PrintedRelations.lift(typed("|let s = 'q'; " + REL + "->toString()->replace($s, ',');")));
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
        assertEquals(2, lifted.relation().body().size());
    }
}
