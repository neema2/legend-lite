// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.Operand;
import com.legend.sql.RaiseKind;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE EDGE'S WORDS OF A RAISE SITE, FROM ITS RECORD (stage B's B1, 2026-11-20, errors as records): legend-pure's
 * sentence of each kind, worded in Java after the run from the site's kind and operands -- a literal printed as
 * legend-pure prints it, a value only the database held as {@link PureMessages#WITHHELD} (D1, register S389). The plan
 * carries no words; this is the one place they are made.
 */
class PureMessagesWordedTest {

    private static String worded(RaiseKind kind, Operand... operands) {
        return PureMessages.worded(kind, List.of(operands));
    }

    @Test
    void literalOperandsAreWordedAsLegendPurePrintsThem() {
        assertEquals("Cannot divide 5 by zero", worded(RaiseKind.DIVIDE_BY_ZERO, new Operand.Int(5)));
        assertEquals("Division by zero", worded(RaiseKind.DIVIDE_BY_ZERO));
        assertEquals("Cannot cast a collection of size 0 to multiplicity [1]",
                worded(RaiseKind.SIZE_NOT_MULTIPLICITY, new Operand.Text("[1]")));
        assertEquals("Cannot cast a collection of size 2 to multiplicity [1]",
                worded(RaiseKind.SIZE_NOT_MULTIPLICITY, new Operand.Text("[1]"), new Operand.Int(2)));
        assertEquals("The system is trying to get an element at offset 4 where the collection is of size 3",
                worded(RaiseKind.INDEX_OUT_OF_BOUNDS, new Operand.Int(4), new Operand.Int(3)));
        assertEquals("The low bound (3) can't be higher than the high bound (1) in a slice operation",
                worded(RaiseKind.SLICE_BOUNDS_INVERTED, new Operand.Int(3), new Operand.Int(1)));
        assertEquals("Invalid month: 13", worded(RaiseKind.DATE_COMPONENT_INVALID, new Operand.Text("month"),
                new Operand.Int(13)));
        assertEquals("Invalid day: 2016-12-32", worded(RaiseKind.DATE_COMPONENT_INVALID, new Operand.Text("day"),
                new Operand.Int(2016), new Operand.Int(12), new Operand.Int(32)));
        assertEquals("Cast exception: Integer cannot be cast to Date",
                worded(RaiseKind.CAST_IMPOSSIBLE, new Operand.Text("Integer"), new Operand.Text("Date")));
        assertEquals("Range [1, 3) out of bounds for length 2", worded(RaiseKind.SUBSTRING_OUT_OF_RANGE,
                new Operand.Int(1), new Operand.Int(3), new Operand.Int(2)));
    }

    @Test
    void aComputedOperandIsWithheld() {
        assertEquals("Cannot divide " + PureMessages.WITHHELD + " by zero",
                worded(RaiseKind.DIVIDE_BY_ZERO, Operand.Computed.IT));
        assertEquals("Cannot cast a collection of size " + PureMessages.WITHHELD + " to multiplicity [1]",
                worded(RaiseKind.SIZE_NOT_MULTIPLICITY, new Operand.Text("[1]"), Operand.Computed.IT));
        assertEquals("Invalid hour: " + PureMessages.WITHHELD,
                worded(RaiseKind.DATE_COMPONENT_INVALID, new Operand.Text("hour"), Operand.Computed.IT));
    }

    /** Every kind a site can carry is worded (the switch is exhaustive by javac; this reads each arm once over
     *  computed operands, so a kind whose arm prints an operand it was not given fails by name); a user's {@code fail}
     *  is its own message, never a site. */
    @Test
    void everySiteKindIsWorded_aUsersFailIsNone() {
        for (RaiseKind kind : RaiseKind.values()) {
            if (kind == RaiseKind.USER_FAIL) {
                assertThrows(IllegalStateException.class, () -> worded(kind, new Operand.Text("mine")));
                continue;
            }
            List<Operand> computed = java.util.Collections.nCopies(4, Operand.Computed.IT);
            String words = PureMessages.worded(kind, kind == RaiseKind.DATE_COMPONENT_INVALID
                    ? List.of(new Operand.Text("hour"), Operand.Computed.IT) : computed);
            assertFalse(words.isBlank(), kind.name());
            assertFalse(words.contains(kind.name()), kind + ": the words are a sentence, never the kind's name: " + words);
        }
    }

    /** The plan carries a site's facts, no words: legend-pure's sentence is the same function of the record wherever
     *  the edge words it (a caught error, pure/v1's answer, PCT's harness). */
    @Test
    void aSitesWordsAreAFunctionOfItsRecordAlone() {
        com.legend.executionplan.ExecutionPlan.Site site = new com.legend.executionplan.ExecutionPlan.Site(1,
                RaiseKind.DATE_COMPONENT_INVALID, "month", List.of(new Operand.Text("month"), new Operand.Int(13)));
        assertEquals("Invalid month: 13", PureMessages.worded(site));
        assertTrue(site.productSentence().endsWith(": month"), site.productSentence());
        assertFalse(site.productSentence().contains("Invalid month"), site.productSentence());
    }
}
