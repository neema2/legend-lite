// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
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
 * legend-pure prints it, a value only the database held as {@link com.legend.result.RaiseWords#COMPUTED} (D1, register S389). The plan
 * carries no words; this is the one place they are made.
 */
class PureMessagesWordedTest {

    private static String worded(RaiseKind kind, Operand... operands) {
        return com.legend.result.RaiseWords.sentence(new ExecutionPlan.Site(1, kind, List.of(operands)));
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
        assertEquals("Cannot divide " + com.legend.result.RaiseWords.COMPUTED + " by zero",
                worded(RaiseKind.DIVIDE_BY_ZERO, Operand.Computed.IT));
        assertEquals("Cannot cast a collection of size " + com.legend.result.RaiseWords.COMPUTED + " to multiplicity [1]",
                worded(RaiseKind.SIZE_NOT_MULTIPLICITY, new Operand.Text("[1]"), Operand.Computed.IT));
        assertEquals("Invalid hour: " + com.legend.result.RaiseWords.COMPUTED,
                worded(RaiseKind.DATE_COMPONENT_INVALID, new Operand.Text("hour"), Operand.Computed.IT));
    }

    /** Every kind a site can carry is worded (the switch is exhaustive by javac; this reads each arm once over
     *  computed operands, so a kind whose arm prints an operand it was not given fails by name); a user's {@code fail}
     *  is its own message, never a site. */
    @Test
    void everySiteKindIsWorded_aUsersFailIsNone() {
        for (RaiseKind kind : RaiseKind.values()) {
            if (kind == RaiseKind.USER_FAIL) {
                assertThrows(IllegalArgumentException.class, () -> worded(kind, new Operand.Text("mine")));
                continue;
            }
            // every form of the kind, given the values it names (the BE audit 2's S5, 2026-11-21: a form takes no
            // fewer values than it names, an exact form its arity alone), a form chosen by an empty value given one
            for (com.legend.error.ErrorKind.Form form : com.legend.error.ErrorKind.valueOf(kind.name()).forms()) {
                int named = 0;
                java.util.regex.Matcher m = java.util.regex.Pattern.compile("%(\\d+)\\$s").matcher(form.template());
                while (m.find()) {
                    named = Math.max(named, Integer.parseInt(m.group(1)));
                }
                int size = form.arity() >= 0 ? form.arity() : Math.max(named, form.emptySlot() + 1);
                List<Operand> values = new java.util.ArrayList<>(java.util.Collections.nCopies(size,
                        Operand.Computed.IT));
                if (form.emptySlot() >= 0) {
                    values.set(form.emptySlot(), new Operand.Text(""));
                }
                String words = com.legend.result.RaiseWords.sentence(new ExecutionPlan.Site(1, kind, values));
                assertFalse(words.isBlank(), kind.name());
                assertFalse(words.contains(kind.name()), kind + ": the words are a sentence, never the kind's name: "
                        + words);
            }
        }
    }

    /** A raise's carriage copied off its record (no site to word) reads its kind's name, as on main: pinned, a residue
     *  the one error model removes (B1's fixes' review, 2026-11-20). */
    @Test
    void aMarkedMessageOffItsRecordReadsItsKindsName_asOnMain() {
        assertEquals("DIVIDE_BY_ZERO", PureMessages.compile(new IllegalStateException(RaiseKind.MARK + "DIVIDE_BY_ZERO"
                + RaiseKind.MARK + "the where")));
        assertEquals("DIVIDE_BY_ZERO", PureMessages.compile(new IllegalStateException(RaiseKind.MARK + "DIVIDE_BY_ZERO")));
    }

    /** The plan carries a site's facts, no words: legend-pure's sentence is the same function of the record wherever
     *  the edge words it (a caught error, pure/v1's answer, PCT's harness). */
    @Test
    void aSitesWordsAreAFunctionOfItsRecordAlone() {
        com.legend.executionplan.ExecutionPlan.Site site = new com.legend.executionplan.ExecutionPlan.Site(1,
                RaiseKind.DATE_COMPONENT_INVALID, List.of(new Operand.Text("month"), new Operand.Int(13)));
        assertEquals("Invalid month: 13", PureMessages.worded(site));
        // one catalog (stage B's BE, 2026-11-21): the product says the same sentence, its place a fact apart
        assertEquals("Invalid month: 13", com.legend.result.RaiseWords.sentence(site));
        // a kind with no upstream text: lite's own sentence over its facts (stage B's BE-P, 2026-11-21)
        com.legend.executionplan.ExecutionPlan.Site own = new com.legend.executionplan.ExecutionPlan.Site(1,
                RaiseKind.INTEGER_PAST_64_BITS, List.of(new Operand.Text("SQLite")));
        assertEquals("an integer past 64 bits has no SQLite value", PureMessages.worded(own));
    }
}
