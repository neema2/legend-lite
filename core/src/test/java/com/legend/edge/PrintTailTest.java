// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE TAIL OF A PRINT: ITS CLOSED SET, PINNED, AND EACH MEMBER'S MEANING (the edition program's step 8, 2026-11-26; the
 * user's words that day, "maybe small set up string/count functions chained should all be in java"). A member is a
 * function of the text alone whose Java here is legend-pure's own Java for it (legend-pure 5.99.0, read 2026-11-26:
 * {@code CompiledSupport} lines 1531-1569, {@code CoreHelper} lines 1209-1234), with no error case and nothing that
 * depends on the machine. The set moves only with the user's word, in a dated note here.
 */
class PrintTailTest {

    /** The members, exactly: {@code replace} since 2026-11-24 (the user: "also do the replace in java too"); the rest
     *  ruled by the user on 2026-11-26 (~13:57), put to them as these seven with the guard exception they need (Java
     *  computes a length, an index or a true/false over text the edge printed): "yes should add guard". {@code +}
     *  with literal Strings is not a member. */
    @Test
    void theClosedSetIsExactlyThese() {
        assertEquals(new TreeSet<>(List.of("Contains", "EndsWith", "IndexOf", "Length", "Replace", "StartsWith", "Trim")),
                new TreeSet<>(Arrays.stream(PrintTail.class.getPermittedSubclasses()).map(Class::getSimpleName)
                        .toList()));
    }

    /** Each member as legend-pure's Java answers it. */
    @Test
    void eachMemberIsLegendPuresJava() {
        // replace: every occurrence, left to right; an empty target inserts the replacement at every position
        assertEquals("a,b,", new PrintTail.Replace("\n", ",").applied("a\nb\n"));
        assertEquals("xax", new PrintTail.Replace("", "x").applied("a"));
        // length: UTF-16 units (a character outside the basic plane counts two, as legend-pure's String.length does)
        assertEquals(3L, new PrintTail.Length().applied("a😀"));
        assertEquals(0L, new PrintTail.Length().applied(""));
        // trim: every character up to U+0020 at either end, a tab and a line break among them; inner ones stay
        assertEquals("a b", new PrintTail.Trim().applied(" \t\na b\r\n "));
        assertEquals(true, new PrintTail.StartsWith("#TDS").applied("#TDS\n"));
        assertEquals(false, new PrintTail.StartsWith("x").applied(""));
        assertEquals(true, new PrintTail.EndsWith("").applied("a"));
        assertEquals(true, new PrintTail.Contains("b").applied("abc"));
        assertEquals(false, new PrintTail.Contains("B").applied("abc"));
        // indexOf: -1 where absent; a start out of range is clamped, never an error
        assertEquals(1L, new PrintTail.IndexOf("b", null).applied("abcb"));
        assertEquals(-1L, new PrintTail.IndexOf("z", null).applied("abc"));
        assertEquals(3L, new PrintTail.IndexOf("b", 2L).applied("abcb"));
        assertEquals(1L, new PrintTail.IndexOf("b", -5L).applied("abcb"));
        assertEquals(-1L, new PrintTail.IndexOf("b", 99L).applied("abcb"));
    }

    /** WHERE JAVA AND A DATABASE PART, Java's answer is legend-pure's own (the independent audit of slice 2,
     *  2026-11-26, its list; each read against legend-pure 5.99.0's Java and run on the build's JDK): a character
     *  above U+FFFF, an empty needle, a control character at an end, a start outside the text. A database counts the
     *  first as one character, and lite's SQL strips spaces alone and answers -1 for a start past the end
     *  (what each database answers beside these is recorded, one answer a case: {@code EdgeCasesGoldenTest}'s
     *  {@code tail.*} cases). */
    @Test
    void whereADatabaseAnswersOtherwiseJavasIsLegendPures() {
        String astral = "a\uD83D\uDE00b";
        assertEquals(4L, new PrintTail.Length().applied(astral));
        assertEquals(3L, new PrintTail.IndexOf("b", null).applied(astral));
        assertEquals(0L, new PrintTail.IndexOf("", null).applied("abc"));
        assertEquals(3L, new PrintTail.IndexOf("", 5L).applied("abc"));
        assertEquals(true, new PrintTail.StartsWith("").applied("abc"));
        assertEquals(true, new PrintTail.EndsWith("").applied("abc"));
        assertEquals(true, new PrintTail.Contains("").applied("abc"));
        assertEquals("xaxbxcx", new PrintTail.Replace("", "x").applied("abc"));
        assertEquals("a", new PrintTail.Trim().applied("\u0000a\u001f"));
        // a no-break space is above U+0020: not stripped
        assertEquals("\u00a0a", new PrintTail.Trim().applied("\u00a0a"));
        // a start past an int wraps as legend-pure's intValue() does, to a negative, which Java clamps to the start
        assertEquals(1L, new PrintTail.IndexOf("b", 2147483648L).applied("abcb"));
        assertEquals(-1L, new PrintTail.IndexOf("b", 2147483647L).applied("abcb"));
    }

    /** A tail is applied in the program's order, and answers its last step's type. */
    @Test
    void aTailIsAppliedInOrderAndAnswersItsLastStepsType() {
        List<PrintTail> tail = List.of(new PrintTail.Replace("\n", ","), new PrintTail.Trim(),
                new PrintTail.Length());
        assertEquals(5L, PrintTail.applied(" a\nb\nc ", tail));
        assertEquals("Integer", PrintTail.answers(tail));
        assertEquals("String", PrintTail.answers(List.of()));
        assertEquals("String", PrintTail.answers(tail.subList(0, 2)));
        assertEquals("Boolean", PrintTail.answers(List.of(new PrintTail.Contains("a"))));
        assertEquals("a", PrintTail.applied("a", List.of()));
    }
}
